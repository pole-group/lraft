// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"

	polerpc "github.com/pole-group/pole-rpc"
)

type JobSwitch int32

const (
	OpenJob JobSwitch = iota
	Suspend
)

type JobType int32

const (
	JobForVote JobType = iota
	JobForElection
	JobForSnapshot
	JobForStepDown
)

type repeatJob interface {
	//start 任务启动
	start()
	//stop 任务不执行
	stop()
}

type RaftNodeJobManager struct {
	node        *nodeImpl
	voteJob     repeatJob
	electionJob repeatJob
	stepDownJob repeatJob
	snapshotJob repeatJob
}

func NewRaftNodeJobManager(node *nodeImpl) *RaftNodeJobManager {
	mgn := &RaftNodeJobManager{
		node: node,
	}

	mgn.electionJob = newElector(node, mgn)
	mgn.snapshotJob = newSnapshotJob(node, mgn)
	mgn.voteJob = newVoteJob(node, mgn)
	mgn.stepDownJob = newStepDownJob(node, mgn)
	return mgn
}

func (mgn *RaftNodeJobManager) startJob(jType JobType) {
	switch jType {
	case JobForVote:
		if mgn.voteJob != nil {
			mgn.voteJob.start()
		}
	case JobForElection:
		if mgn.electionJob != nil {
			mgn.electionJob.start()
		}
	case JobForSnapshot:
		if mgn.snapshotJob != nil {
			mgn.snapshotJob.start()
		}
	case JobForStepDown:
		if mgn.stepDownJob != nil {
			mgn.stepDownJob.start()
		}
	}
}

func (mgn *RaftNodeJobManager) stopJob(jType JobType) {
	switch jType {
	case JobForVote:
		if mgn.voteJob != nil {
			mgn.voteJob.stop()
		}
	case JobForElection:
		if mgn.electionJob != nil {
			mgn.electionJob.stop()
		}
	case JobForSnapshot:
		if mgn.snapshotJob != nil {
			mgn.snapshotJob.stop()
		}
	case JobForStepDown:
		if mgn.stepDownJob != nil {
			mgn.stepDownJob.stop()
		}
	}
}

func (mgn *RaftNodeJobManager) shutdown() {
	mgn.voteJob.stop()
	mgn.snapshotJob.stop()
	mgn.electionJob.stop()
	mgn.stepDownJob.stop()
}

type VoteJob struct {
	jobMgn   *RaftNodeJobManager
	node     *nodeImpl
	lock     *sync.RWMutex
	future   polerpc.Future
	stopSign JobSwitch
}

func newVoteJob(node *nodeImpl, mgn *RaftNodeJobManager) *VoteJob {
	return &VoteJob{
		jobMgn:   mgn,
		node:     node,
		lock:     node.lock,
		stopSign: 0,
	}
}

func (v *VoteJob) start() {
	atomic.StoreInt32((*int32)(&v.stopSign), int32(OpenJob))
	v.initVoteJob()
}

func (v *VoteJob) stop() {
	atomic.StoreInt32((*int32)(&v.stopSign), int32(Suspend))
	v.future.Cancel()
}

// initVoteJob 初始化投票的定时任务
func (v *VoteJob) initVoteJob() {
	v.future = polerpc.DoTimerSchedule(func() {
		// 如果当前任务可执行的状态依旧 open 状态的话，则继续执行处理
		if atomic.LoadInt32((*int32)(&v.stopSign)) == int32(OpenJob) {
			// 如果到了指定的超时时间
			v.handleVoteTimeout()
		}
	}, time.Duration(v.node.options.ElectionTimeoutMs)*time.Millisecond, func() time.Duration {
		return time.Duration(v.node.options.ElectionTimeoutMs+rand.Int63n(v.node.options.ElectionMaxDelayMs)) * time.Millisecond
	})
}

//handleVoteTimeout 投票超时任务处理，在规定的时间范围内没有收集到足够多的票数让自己成为 Leader
func (v *VoteJob) handleVoteTimeout() {
	v.lock.Lock()
	// 如果自己的状态没有成为 Candidate，则不能开启自己竞选 Leader 的动作
	if v.node.state != StateCandidate {
		v.lock.Unlock()
		return
	}

	if v.node.raftOptions.StepDownWhenVoteTimeout {
		stepDown(v.node, v.node.currTerm, false, entity.NewStatus(entity.ETimeout,
			"vote timeout: fail to get quorum vote-granted"))
		doPreVote(v.node)
	} else {
		utils.RaftLog.Debug("node %s term %d retry to vote self", v.node.nodeID.GetDesc(), v.node.currTerm)
		electSelf(v.node)
	}
}

type ElectionJob struct {
	jobMgn      *RaftNodeJobManager
	node        *nodeImpl
	electionCnt int32
	lock        *sync.RWMutex
	stopSign    JobSwitch
	future      polerpc.Future
}

func newElector(node *nodeImpl, mgn *RaftNodeJobManager) *ElectionJob {
	return &ElectionJob{
		jobMgn: mgn,
		node:   node,
		lock:   node.lock,
	}
}

func (el *ElectionJob) start() {
	atomic.StoreInt32((*int32)(&el.stopSign), int32(OpenJob))
	el.initElectionJob()
}

func (el *ElectionJob) stop() {
	atomic.StoreInt32((*int32)(&el.stopSign), int32(Suspend))
	el.future.Cancel()
}

//initElectionJob 处理来自 Leader 的心跳包数据，判断如果 Leader 超过多久没有向自己续约 Leader 信息的话，就会开启 preVote 机制先判断是否可以竞争 Leader
//，同时由于是采用了 preVote，避免了 term 可能会疯狂上涨的问题
func (el *ElectionJob) initElectionJob() {
	el.future = polerpc.DoTimerSchedule(func() {
		if atomic.LoadInt32((*int32)(&el.stopSign)) == int32(OpenJob) {
			el.handleElectionTimeout()
		}
	}, time.Duration(el.node.options.ElectionTimeoutMs)*time.Millisecond, func() time.Duration {
		return time.Duration(el.node.options.ElectionTimeoutMs+rand.Int63n(el.node.options.ElectionMaxDelayMs)) * time.Millisecond
	})
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

	// 当前 Leader 在自己这里的租约是否过期了
	if el.node.currentLeaderIsValid() {
		return
	}

	//重置当前的 Leader 信息
	el.node.resetLeaderId(entity.EmptyPeer, entity.NewStatus(entity.ERaftTimedOut,
		fmt.Sprintf("lost connection from leader %s", el.node.leaderID.GetDesc())))

	// 判断当前自己是否可以进行 Leader 的竞选
	if !el.allowLaunchElection() {
		return
	}
	doUnlock = false

	// 开始做预投票
	doPreVote(el.node)
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

type SnapshotJob struct {
	firstSchedule bool
	jobMgn        *RaftNodeJobManager
	node          *nodeImpl
	lock          *sync.RWMutex
	stopSign      JobSwitch
	snapshotSign  chan int8
	future        polerpc.Future
}

func newSnapshotJob(node *nodeImpl, mgn *RaftNodeJobManager) repeatJob {
	return &SnapshotJob{
		firstSchedule: true,
		jobMgn:        mgn,
		node:          node,
		lock:          node.lock,
		stopSign:      0,
		snapshotSign:  make(chan int8, 1),
	}
}

//start 任务启动
func (sj *SnapshotJob) start() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(OpenJob))

	polerpc.Go(nil, func(arg interface{}) {
		for range sj.snapshotSign {
		}
	})

	sj.future = polerpc.DoTimerSchedule(func() {
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

//stop 任务不执行
func (sj *SnapshotJob) stop() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(Suspend))
	close(sj.snapshotSign)
	sj.future.Cancel()
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

type StepDownJob struct {
	node     *nodeImpl
	jogMgn   *RaftNodeJobManager
	lock     *sync.RWMutex
	future   polerpc.Future
	stopSign JobSwitch
}

func newStepDownJob(node *nodeImpl, mgn *RaftNodeJobManager) *StepDownJob {
	return &StepDownJob{
		node:   node,
		jogMgn: mgn,
		lock:   node.lock,
	}
}

//start 任务启动
func (sj *StepDownJob) start() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(OpenJob))
	sj.future = polerpc.DelaySchedule(func() {
		if atomic.LoadInt32((*int32)(&sj.stopSign)) == int32(OpenJob) {
			sj.handleStepDownTimeout()
		}
	}, time.Duration(sj.node.options.ElectionTimeoutMs>>1)*time.Millisecond)
}

//stop 任务不执行
func (sj *StepDownJob) stop() {
	atomic.StoreInt32((*int32)(&sj.stopSign), int32(Suspend))
	sj.future.Cancel()
}

func (sj *StepDownJob) handleStepDownTimeout() {
	node := sj.node
	checkDeadNodes := func(lock sync.Locker) bool {
		defer lock.Unlock()
		lock.Lock()
		if node.state > StateTransferring {
			utils.RaftLog.Debug("node %s stop step-down timer, term=%d, state=%s.", node.nodeID.GetDesc(),
				node.currTerm, node.state.GetName())
			return false
		}

		nowMs := time.Now()
		if !node.checkDeadNodes(node.conf.GetConf(), nowMs, false) {
			return false
		}

		if !node.conf.GetOldConf().IsEmpty() {
			if !node.checkDeadNodes(node.conf.GetOldConf(), nowMs, false) {
				return false
			}
		}
		return true
	}

	// 先是进行乐观的检查
	if checkDeadNodes(sj.lock.RLocker()) {
		return
	}
	// 进行悲观锁检查
	checkDeadNodes(sj.lock)
}

//electSelf 通过 preVote 之后，就开始真正的将自己的term上调并进行Leader的竞选
func electSelf(node *nodeImpl) {
	utils.RaftLog.Info("node %s startJob vote and grant vote self, term=%d.", node.nodeID.GetDesc(), node.currTerm)

	startVote := func() (bool, int64) {
		defer node.lock.Unlock()
		// 自己不再集群列表里面，不可以发起选举！
		if !node.conf.ContainPeer(node.serverID) {
			utils.RaftLog.Warn("node %s can't do electSelf as it is not in %#v.", node.nodeID.GetDesc(), node.conf)
			return false, -1
		}

		if node.state == StateFollower {
			utils.RaftLog.Debug("node %s stop election timer, term=%d", node.nodeID.GetDesc(), node.currTerm)
			node.raftNodeJobMgn.stopJob(JobForElection)
		}
		// 因为自己的状态提升为了 StateCandidate，因此自己不认当前的 Leader，直接将自己原来记住的 Leader 信息丢弃
		node.resetLeaderId(entity.EmptyPeer, entity.NewStatus(entity.ERaftTimedOut,
			"a follower's leader_id is reset to NULL as it begins to request_vote."))
		node.state = StateCandidate
		node.currTerm++
		// 将票投给自己
		node.votedId = node.serverID.Copy()
		utils.RaftLog.Debug("node %s startJob vote timer, term=%d", node.nodeID.GetDesc(), node.currTerm)

		// 开启 vote 的超时计算任务，自己必须在规定的时间内获取到半数投票才可以
		node.raftNodeJobMgn.startJob(JobForVote)

		var oldConf *entity.Configuration
		if !node.conf.IsStable() {
			oldConf = node.conf.GetOldConf()
		}

		node.voteCtx.Init(node.conf.GetConf(), oldConf)
		return true, node.currTerm
	}

	isGoon, oldTerm := startVote()
	if !isGoon {
		return
	}

	// 在准备发起投票时，必须确保自己的已经接收到 Log 都已经持久化到磁盘了，这个时候才可以去获取 <term, logIndex> 信息
	lastLogId := node.logManager.GetLastLogID(true)

	defer node.lock.Unlock()
	node.lock.Lock()

	if node.currTerm != oldTerm {
		utils.RaftLog.Warn("node %s raise term {} when getLastLogId.", node.nodeID.GetDesc())
		return
	}
	node.conf.ListPeers().Range(func(value interface{}) {
		peer := value.(entity.PeerId)
		if peer.Equal(node.serverID) {
			return
		}
		if ok, err := node.raftOperator.raftClient.CheckConnection(peer.GetEndpoint()); !ok || err != nil {
			utils.RaftLog.Warn("node %s channel init failed, address=%s", node.nodeID.GetDesc(), peer.GetEndpoint().GetDesc())
			return
		}
		done := &OnRequestVoteRpcDone{
			PeerId:    peer,
			Term:      node.currTerm,
			node:      node,
			StartTime: time.Now(),
			Req: &raft.RequestVoteRequest{
				GroupID:      node.groupID,
				ServerID:     node.serverID.GetDesc(),
				PeerID:       peer.GetDesc(),
				Term:         node.currTerm,
				LastLogTerm:  lastLogId.GetTerm(),
				LastLogIndex: lastLogId.GetIndex(),
				PreVote:      false,
			},
		}

		done.F = func(resp proto.Message, status entity.Status) {
			if status.IsOK() {
				handleRequestVoteResponse(node, done.PeerId, done.Term, done.Resp.(*raft.RequestVoteResponse))
			} else {
				utils.RaftLog.Warn("node : %s request vote to : %s error : %s", node.nodeID.GetDesc(),
					done.PeerId.GetDesc(), status.GetMsg())
			}
		}
		node.raftOperator.RequestVote(peer.GetEndpoint(), done.Req, done)
	})

	// 保存元数据信息
	node.metaStorage.setTermAndVotedFor(node.currTerm, node.serverID)
	node.voteCtx.Grant(node.serverID)
	// 如果当前已经有超过半数的 Follower 同意了自己的 Leader 竞争选举，那么就正式成为 Leader
	if node.voteCtx.IsGrant() {
		_ = becomeLeader(node)
	}
}

//doPreVote 为了避免 Term 因为选举失败而导致 term 上涨的问题，这里做了优化，采用预投票的方式，先试探一下自己是否可以竞争为 Leader,
//如果可以的话, 在执行真正的 Vote 机制
func doPreVote(node *nodeImpl) {
	utils.RaftLog.Info("node : %s term : %d startJob preVote", node.nodeID.GetDesc(), node.currTerm)
	if node.snapshotExecutor != nil && node.snapshotExecutor.IsInstallingSnapshot() {
		utils.RaftLog.Warn("node : %s term : %d doesn't do preVote when installing snapshot as the configuration may" +
			" be out of date")
		return
	}
	if !node.conf.ContainPeer(node.serverID) {
		return
	}
	oldTerm := node.currTerm
	node.lock.Unlock()

	// 首先要保证自己当前所有已经接收到的 LogEntry 都已经 flush 到了持久化存储介质中
	lastLogId := node.logManager.GetLastLogID(true)
	doUnlock := true

	defer func() {
		if doUnlock {
			node.lock.Unlock()
		}
	}()
	node.lock.Lock()

	if oldTerm != node.currTerm {
		utils.RaftLog.Warn("node : %s raise term : %d when get lastLogId", node.nodeID.GetDesc(), node.currTerm)
		return
	}
	var oldConf *entity.Configuration
	if node.conf.IsStable() {
		oldConf = node.conf.GetOldConf()
	}
	node.preVoteCtx.Init(node.conf.GetConf(), oldConf)
	node.conf.ListPeers().Range(func(value interface{}) {
		peer := value.(entity.PeerId)
		if peer.Equal(node.serverID) {
			return
		}
		if ok, err := node.raftOperator.raftClient.CheckConnection(peer.GetEndpoint()); !ok || err != nil {
			utils.RaftLog.Warn("node %s channel init failed, address=%s", node.nodeID.GetDesc(), peer.GetEndpoint().GetDesc())
			return
		}
		done := &OnPreVoteRpcDone{
			PeerId:    peer,
			Term:      node.currTerm,
			StartTime: time.Now(),
			Req: &raft.RequestVoteRequest{
				GroupID:      node.groupID,
				ServerID:     node.serverID.GetDesc(),
				PeerID:       peer.GetDesc(),
				Term:         node.currTerm + 1,
				LastLogTerm:  lastLogId.GetTerm(),
				LastLogIndex: lastLogId.GetIndex(),
				PreVote:      true,
			},
		}

		done.F = func(resp proto.Message, status entity.Status) {
			if status.IsOK() {
				handlePreVoteResponse(node, done.PeerId, done.Term, done.Resp.(*raft.RequestVoteResponse))
			} else {
				utils.RaftLog.Warn("node : %s pre vote to : %s error : %s", node.nodeID.GetDesc(),
					done.PeerId.GetDesc(), status.GetMsg())
			}
		}

		node.raftOperator.PreVote(peer.GetEndpoint(), done.Req, done)
	})
	node.preVoteCtx.Grant(node.serverID)
	if node.preVoteCtx.IsGrant() {
		doUnlock = false
		electSelf(node)
	}
}

// handleRequestVoteResponse
func handleRequestVoteResponse(node *nodeImpl, peer entity.PeerId, term int64, resp *raft.RequestVoteResponse) {
	defer node.lock.Unlock()
	node.lock.Lock()

	if node.state != StateCandidate {
		utils.RaftLog.Warn("Node %s received invalid RequestVoteResponse from %s, state not in StateCandidate but %s.",
			node.nodeID.GetDesc(), peer.GetDesc(), node.state.GetName())
		return
	}

	if term != node.currTerm {
		return
	}
	if resp.Term > term {
		stepDown(node, resp.Term, false, entity.NewStatus(entity.EHigherTermResponse,
			"Raft node receives higher term RequestVoteResponse."))
		return
	}
	if resp.Granted {
		node.voteCtx.Grant(peer)
		if node.voteCtx.IsGrant() {
			_ = becomeLeader(node)
		}
	}
}

// handlePreVoteResponse
func handlePreVoteResponse(node *nodeImpl, peer entity.PeerId, term int64, resp *raft.RequestVoteResponse) {
	doUnlock := true

	defer func() {
		if doUnlock {
			node.lock.Unlock()
		}
	}()

	node.lock.Lock()

	if node.state != StateFollower {
		utils.RaftLog.Warn("Node %s received invalid PreVoteResponse from %s, state not in StateFollower but %s.",
			node.nodeID.GetDesc(), peer.GetDesc(), node.state.GetName())
		return
	}

	if term != node.currTerm {
		return
	}
	if resp.Term > term {
		stepDown(node, resp.Term, false, entity.NewStatus(entity.EHigherTermResponse,
			"Raft node receives higher term pre_vote_response."))
		return
	}
	if resp.Granted {
		node.preVoteCtx.Grant(peer)
		if node.preVoteCtx.IsGrant() {
			doUnlock = false
			electSelf(node)
		}
	}
}

// stepDown 停止自己的一些任务，只有在出现状态转换的时候需要做这个动作，并且是从 Leader 降级为 Follower
func stepDown(node *nodeImpl, term int64, wakeupCandidate bool, status entity.Status) {
	if !IsNodeActive(node.state) {
		return
	}

	// 自己处于 Candidate 状态的时候，就不能够在进行投票的操作了
	if node.state == StateCandidate {
		node.raftNodeJobMgn.stopJob(JobForVote)
	} else if node.state <= StateTransferring {
		node.raftNodeJobMgn.stopJob(JobForStepDown)
		node.ballotBox.ClearPendingTasks()
		if node.state == StateLeader {
			node.onLeaderStop(status)
		}
	}
	// 自己不是Leader了，清空自己的 Leader 信息数据，并将自己的状态更改为 Follower
	node.resetLeaderId(entity.EmptyPeer, status)
	node.state = StateFollower
	// 清空自己的配置信息，这个信息只能以 Leader 的为准
	node.confCtx.Reset(status)
	node.lastLeaderTimestamp = utils.GetCurrentTimeMs()
	if node.snapshotExecutor != nil {
		node.snapshotExecutor.stopDownloadingSnapshot(term)
	}
	if term > node.currTerm {
		node.currTerm = term
		node.votedId = entity.EmptyPeer
		node.metaStorage.setTermAndVotedFor(term, node.votedId)
	}

	if wakeupCandidate {
		node.wakingCandidate = node.replicatorGroup.stopAllAndFindTheNextCandidate(node.conf)
		if node.wakingCandidate != nil {
			node.replicatorGroup.sendTimeoutNowAndStop(node.wakingCandidate, node.options.ElectionTimeoutMs)
		}
	} else {
		node.replicatorGroup.stopAll()
	}
	if node.stopTransferArg != nil {
		if node.transferFuture != nil {
			node.transferFuture.Cancel()
		}
		node.stopTransferArg = nil
	}
	if !node.IsLearner() {
		node.raftNodeJobMgn.startJob(JobForElection)
	} else {
		utils.RaftLog.Info("node %s is a learner, election timer is not started.", node.nodeID.GetDesc())
	}
}

func becomeLeader(node *nodeImpl) error {
	if err := utils.RequireTrue(node.state == StateCandidate, "illegal state %s", node.state.GetName()); err != nil {
		panic(err)
	}
	utils.RaftLog.Info("node %s become leader of group, term=%d, conf=%#v, oldConf=%#v.", node.nodeID.GetDesc(),
		node.currTerm, node.conf.GetConf(), node.conf.GetOldConf())
	node.raftNodeJobMgn.stopJob(JobForVote)
	node.state = StateLeader
	node.leaderID = node.serverID.Copy()
	node.replicatorGroup.resetTerm(node.currTerm)

	node.conf.ListPeers().Range(func(value interface{}) {
		peer := value.(entity.PeerId)
		if peer.Equal(node.serverID) {
			return
		}
		utils.RaftLog.Debug("node : %s add a follower replicator, term=%d, peer=%s", node.nodeID.GetDesc(),
			node.currTerm, peer.GetDesc())
		if success, err := node.replicatorGroup.addReplicator(peer, ReplicatorFollower, true); !success || err != nil {
			utils.RaftLog.Error("fail to add a follower replicator, peer %s, err %s", peer.GetDesc(), err)
		}
	})

	node.conf.ListLearners().Range(func(value interface{}) {
		learner := value.(entity.PeerId)
		utils.RaftLog.Debug("node : %s add a learner replicator, term=%d, peer=%s", node.nodeID.GetDesc(),
			node.currTerm, learner.GetDesc())
		if success, err := node.replicatorGroup.addReplicator(learner, ReplicatorLearner, true); !success || err != nil {
			utils.RaftLog.Error("fail to add a learner replicator, peer %s, err %s", learner.GetDesc(), err)
		}
	})

	node.ballotBox.RestPendingIndex(node.logManager.GetLastLogIndex() + 1)
	if node.confCtx.IsBusy() {
		return fmt.Errorf("node conf ctx is busy : %#v", node.confCtx.stage)
	}
	if err := node.confCtx.flush(node.conf.GetConf(), node.conf.GetOldConf()); err != nil {
		return err
	}
	node.raftNodeJobMgn.startJob(JobForStepDown)
	return nil
}

func doSnapshot(node *nodeImpl, done Closure) {

}
