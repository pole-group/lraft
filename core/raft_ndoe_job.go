// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/utils"

	polerpc "github.com/pole-group/pole-rpc"
)

type JobSwitch int32

const (
	OpenJob JobSwitch = iota
	Suspend
)

type repeatJob interface {
	//start 任务启动
	start()
	//restart 任务重启
	restart()
	//stop 任务不执行
	stop()
	//shutdown 任务关闭
	shutdown()
}

type RaftNodeJobManager struct {
	electionChan chan int8
	voteJob      repeatJob
	electionJob  repeatJob
}

func (mgn *RaftNodeJobManager) start() {

}

func (mgn *RaftNodeJobManager) shutdown() {
	close(mgn.electionChan)
	mgn.voteJob.shutdown()
	mgn.electionJob.shutdown()
}

type VoteJob struct {
	jobMgn   *RaftNodeJobManager
	node     *nodeImpl
	lock     *sync.RWMutex
	cancelF  context.CancelFunc
	ctx      context.Context
	stopSign JobSwitch
}

func (v *VoteJob) initVoteJob() {
	ctx, cancelF := context.WithCancel(context.Background())
	v.ctx = ctx
	v.cancelF = cancelF

	polerpc.DoTimerSchedule(ctx, func() {
		if atomic.LoadInt32((*int32)(&v.stopSign)) == int32(OpenJob) {
			v.handleVoteTimeout()
		}
	}, time.Duration(v.node.options.ElectionTimeoutMs)*time.Millisecond, func() time.Duration {
		return time.Duration(v.node.options.ElectionTimeoutMs+rand.Int63n(v.node.options.ElectionMaxDelayMs)) * time.Millisecond
	})
}

func (v *VoteJob) start() {
	atomic.StoreInt32((*int32)(&v.stopSign), int32(OpenJob))
	v.initVoteJob()
}

func (v *VoteJob) restart() {
	atomic.StoreInt32((*int32)(&v.stopSign), int32(OpenJob))
}

func (v *VoteJob) stop() {
	atomic.StoreInt32((*int32)(&v.stopSign), int32(Suspend))
}

func (v *VoteJob) shutdown() {
	v.stop()
	v.cancelF()
}

//handleVoteTimeout 投票超时任务处理，在规定的时间范围内没有收集到足够多的票数让自己成为 Leader
func (v *VoteJob) handleVoteTimeout() {
	v.lock.Lock()
	if v.node.state != StateCandidate {
		v.lock.Unlock()
		return
	}

	if v.node.raftOptions.StepDownWhenVoteTimeout {
		stepDown(v.node.currTerm, false, entity.NewStatus(entity.ETIMEDOUT,
			"vote timeout: fail to get quorum vote-granted"))
		doPreVote(v.node)
	} else {
		utils.RaftLog.Debug("node %s term %d retry to vote self", v.node.nodeID.GetDesc(), v.node.currTerm)
		v.jobMgn.electionChan <- int8(1)
	}
}

type ElectionJob struct {
	jobMgn      *RaftNodeJobManager
	node        *nodeImpl
	electionCnt int32
	lock        *sync.RWMutex
	cancelF     context.CancelFunc
	ctx         context.Context
	stopSign    JobSwitch
}

func newElector(node *nodeImpl) *ElectionJob {
	return &ElectionJob{
		node: node,
		lock: node.rwMutex,
	}
}

func (el *ElectionJob) initElectionJob() {
	ctx, cancelF := context.WithCancel(context.Background())
	el.ctx = ctx
	el.cancelF = cancelF

	polerpc.GoEmpty(func() {
		for range el.jobMgn.electionChan {
			el.electSelf()
		}
	})

	polerpc.DoTimerSchedule(ctx, func() {
		if atomic.LoadInt32((*int32)(&el.stopSign)) == int32(OpenJob) {
			el.handleElectionTimeout()
		}
	}, time.Duration(el.node.options.ElectionTimeoutMs)*time.Millisecond, func() time.Duration {
		return time.Duration(el.node.options.ElectionTimeoutMs+rand.Int63n(el.node.options.ElectionMaxDelayMs)) * time.Millisecond
	})

	el.lock.Lock()
	if el.node.conf.IsStable() && el.node.conf.GetConf().Size() == 1 && el.node.conf.ContainPeer(el.node.serverID) {
		el.jobMgn.electionChan <- int8(1)
	} else {
		el.lock.Unlock()
	}
}

//handleElectionTimeout 接收到 Leader 心跳包的时间超时了, 自己可以向其他节点发起投票请求, 竞争成为 Leader 节点
func (el *ElectionJob) handleElectionTimeout() {
	doUnlock := true
	defer func() {
		if doUnlock {
			el.lock.Unlock()
		}
	}()
	el.lock.Lock()

	if el.node.state != StateFollower {
		return
	}
	if el.node.IsCurrentLeaderValid() {
		return
	}

	//重置当前的 Leader 信息
	el.node.resetLeaderId(entity.EmptyPeer, entity.NewStatus(entity.ERaftTimedOut,
		fmt.Sprintf("lost connection from leader %s", el.node.leaderID.GetDesc())))

	if !el.allowLaunchElection() {
		return
	}
	doPreVote(el.node)
	doUnlock = false
}

func (el *ElectionJob) allowLaunchElection() bool {
	if el.node.serverID.IsPriorityNotElected() {
		utils.RaftLog.Warn("node %s will never participate in election, because priority=%d",
			el.node.serverID.GetDesc(), el.node.serverID.GetPriority())
		return false
	}
	if el.node.serverID.IsPriorityDisabled() {
		return true
	}
	if int32(el.node.serverID.GetPriority()) < el.node.targetPriority {
		el.electionCnt++
		if el.electionCnt > 1 {
			el.decayTargetPriority()
			el.electionCnt = 0
		}
		if el.electionCnt == 1 {
			utils.RaftLog.Warn("node %s does not initiate leader election and waits for the next election"+
				" timeout.", el.node.nodeID.GetDesc())
			return false
		}
	}
	return int32(el.node.serverID.GetPriority()) >= el.node.targetPriority
}

func (el *ElectionJob) decayTargetPriority() {
	decayPriorityGap := int32(math.Max(float64(el.node.options.DecayPriorityGap), float64(10)))
	gap := int32(math.Max(float64(decayPriorityGap), float64(el.node.targetPriority/2)))
	preTargetPriority := el.node.targetPriority
	el.node.targetPriority = int32(math.Max(float64(entity.ElectionPriorityMinValue), float64(el.node.targetPriority-gap)))
	utils.RaftLog.Info("node %s priority decay, from : %d to : %d", el.node.nodeID.GetDesc(), preTargetPriority,
		el.node.targetPriority)
}

func (el *ElectionJob) start() {
	atomic.StoreInt32((*int32)(&el.stopSign), int32(OpenJob))
	el.initElectionJob()
}

func (el *ElectionJob) restart() {
	atomic.StoreInt32((*int32)(&el.stopSign), int32(OpenJob))
}

func (el *ElectionJob) stop() {
	atomic.StoreInt32((*int32)(&el.stopSign), int32(Suspend))
}

func (el *ElectionJob) shutdown() {
	el.stop()
	el.cancelF()
}

func (el *ElectionJob) electSelf() {

}

type SnapshotJob struct {
	firstSchedule bool
	jobMgn        *RaftNodeJobManager
	node          *nodeImpl
	lock          *sync.RWMutex
	cancelF       context.CancelFunc
	ctx           context.Context
	stopSign      JobSwitch
	snapshotSign  chan int8
}

func newSnapshotJob(node *nodeImpl, mgn *RaftNodeJobManager) repeatJob {
	return &SnapshotJob{
		firstSchedule: true,
		jobMgn:        mgn,
		node:          node,
		lock:          node.rwMutex,
		cancelF:       nil,
		ctx:           nil,
		stopSign:      0,
		snapshotSign:  make(chan int8, 1),
	}
}

//start 任务启动
func (sj *SnapshotJob) start() {
	ctx, cancelF := context.WithCancel(context.Background())
	sj.ctx = ctx
	sj.cancelF = cancelF
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(OpenJob))

	polerpc.GoEmpty(func() {
		for range sj.snapshotSign {

		}
	})

	polerpc.DoTimerSchedule(ctx, func() {
		if atomic.LoadInt32((*int32)(&sj.stopSign)) == int32(OpenJob) {
			sj.handleSnapshotTimeout()
		}
	}, time.Duration(sj.node.options.SnapshotIntervalSecs)*time.Second, func() time.Duration {
		if !sj.firstSchedule {
			return time.Duration(sj.node.options.SnapshotIntervalSecs) * time.Second
		}
		sj.firstSchedule = false
		if sj.node.options.SnapshotIntervalSecs > 0 {
			half := sj.node.options.SnapshotIntervalSecs / 2
			return time.Duration(half+rand.Int31n(half)) * time.Second
		} else {
			return time.Duration(sj.node.options.SnapshotIntervalSecs) * time.Second
		}
	})
}

//restart 任务重启
func (sj *SnapshotJob) restart() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(OpenJob))
}

//stop 任务不执行
func (sj *SnapshotJob) stop() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(Suspend))
}

//shutdown 任务关闭
func (sj *SnapshotJob) shutdown() {
	sj.stop()
	sj.cancelF()
}

func (sj *SnapshotJob) handleSnapshotTimeout() {
	sj.lock.Lock()
	if sj.node.state >= StateError {
		sj.lock.Unlock()
		return
	}
	sj.lock.Unlock()
	sj.snapshotSign <- int8(1)
}

//doPreVote 为了避免 Term 因为选举失败而导致不堵上涨的问题，这里做了优化，采用预投票的方式，先试探一下自己是否可以竞争为 Leader,
//如果可以的话, 在执行真正的 Vote 机制
func doPreVote(node *nodeImpl) {

}

func stepDown(term int64, wakeupCandidate bool, status entity.Status) {

}

func becomeLeader(node *nodeImpl) {
	jobMgn := node.raftNodeJobMgn
	jobMgn.voteJob.stop()
}

func doSnapshot(node *nodeImpl, done Closure) {

}
