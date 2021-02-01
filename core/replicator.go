// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"

	"github.com/golang/protobuf/proto"
)

type RunningState int

const (
	Idle RunningState = iota
	Blocking
	AppendingEntries
	InstallingSnapshot
)

type ReplicatorState int32

const (
	ReplicatorProbe ReplicatorState = iota
	ReplicatorSnapshot
	ReplicatorReplicate
	ReplicatorDestroyed
)

type ReplicatorEvent int

const (
	ReplicatorCreatedEvent ReplicatorEvent = iota
	ReplicatorErrorEvent
	ReplicatorDestroyedEvent
)

type RequestType int

const (
	RequestTypeForSnapshot RequestType = iota
	RequestTypeForAppendEntries
)

type ReplicatorStateListener interface {
	OnCreate(peer *entity.PeerId)

	OnError(peer *entity.PeerId, st entity.Status)

	OnDestroyed(peer *entity.PeerId)
}

type Stat struct {
	runningState     RunningState
	firstLogIndex    int64
	lastLogIncluded  int64
	lastLogIndex     int64
	lastTermIncluded int64
}

type InFlight struct {
	reqCnt     int32
	startIndex int64
	size       int32
	future     polerpc.Future
	reqType    RequestType
	seq        int64
}

func (ifl *InFlight) IsSendingLogEntries() bool {
	return ifl.reqType == RequestTypeForAppendEntries && ifl.reqCnt > 0
}

type RpcResponse struct {
	status      entity.Status
	req         *proto.Message
	resp        *proto.Message
	rpcSendTime int64
	seq         int64
	reqType     RequestType
}

func (rp *RpcResponse) Compare(other *RpcResponse) int {
	return int(rp.seq - other.seq)
}

//Replicator 这个对象本身，是一个临界资源，log的发送要是竞争的
type Replicator struct {
	lock                   sync.Locker
	raftOperator           *RaftClientOperator
	nextIndex              int64
	hasSucceeded           bool
	consecutiveErrorTimes  int64
	timeoutNowIndex        int64
	lastRpcSendTimestamp   int64
	heartbeatCounter       int64
	appendEntriesCounter   int64
	installSnapshotCounter int64
	statInfo               Stat
	state                  ReplicatorState
	reqSeq                 int64
	requiredNextSeq        int64
	version                int32
	seqGenerator           int64
	waitId                 int64
	rpcInFly               *InFlight
	inFlights              list.List // <*InFlight>
	options                *replicatorOptions
	raftOptions            RaftOptions
	heartbeatInFly         polerpc.Future
	timeoutNowInFly        polerpc.Future
	destroy                bool
	futures                *polerpc.ConcurrentSlice
}

func NewReplicator(opts *replicatorOptions, raftOpts RaftOptions) *Replicator {
	return &Replicator{
		lock:         &sync.Mutex{},
		options:      opts,
		raftOptions:  raftOpts,
		nextIndex:    opts.logMgn.GetLastLogIndex() + 1,
		raftOperator: opts.raftRpcOperator,
		futures:      &polerpc.ConcurrentSlice{},
	}
}

func (r *Replicator) Start() (bool, error) {
	if ok, err := r.raftOperator.raftClient.CheckConnection(r.options.peerId.GetEndpoint()); !ok || err != nil {
		utils.RaftLog.Error("fail init sending channel to %s", r.options.peerId.GetDesc())
		return ok, err
	}
	r.lock.Lock()
	notifyReplicatorStatusListener(r, ReplicatorCreatedEvent, entity.NewEmptyStatus())
	utils.RaftLog.Info("replicator=%#v@%s is started", r, r.options.peerId.GetDesc())
	r.lastRpcSendTimestamp = utils.GetCurrentTimeMs()
	r.startHeartbeat(utils.GetCurrentTimeMs())
	r.sendEmptyEntries(false, nil)
	return true, nil
}

func (r *Replicator) Stop() {

}

//AddInFlights
func (r *Replicator) AddInFlights(reqType RequestType, startIndex int64, cnt, size int32, seq int64,
	rpcInFly polerpc.Future) {
	r.rpcInFly = &InFlight{
		reqType:    reqType,
		startIndex: startIndex,
		reqCnt:     cnt,
		size:       size,
		seq:        seq,
		future:     rpcInFly,
	}
	r.inFlights.PushBack(r.rpcInFly)
	// TODO metrics
}

//GetNextSendIndex
func (r *Replicator) GetNextSendIndex() int64 {
	if r.inFlights.Len() == 0 {
		return r.nextIndex
	}
	if int64(r.inFlights.Len()) > r.raftOptions.MaxReplicatorInflightMs {
		return -1
	}
	if r.rpcInFly != nil && r.rpcInFly.IsSendingLogEntries() {
		return r.rpcInFly.startIndex + int64(r.rpcInFly.reqCnt)
	}
	return -1
}

//pollInFlight
func (r *Replicator) pollInFlight() *InFlight {
	v := r.inFlights.Front()
	r.inFlights.Remove(v)
	return v.Value.(*InFlight)
}

//startHeartbeat 当
func (r *Replicator) startHeartbeat(startMs int64) {
	dueTime := startMs + int64(r.options.dynamicHeartBeatTimeoutMs)
	future := polerpc.DoTimerSchedule(func() {
		// 实际这里会触发的是 sendHeartbeat 的操作
		r.setError(entity.ETIMEDOUT)
	}, time.Duration(dueTime)*time.Millisecond, func() time.Duration {
		return time.Duration(dueTime) * time.Millisecond
	})
	r.futures.Add(future)
}

func (r *Replicator) setError(errCode entity.RaftErrorCode) {
	if r.destroy {
		return
	}
	r.lock.Lock()
	if r.destroy {
		r.lock.Unlock()
		return
	}
	onError(r, errCode)
}

//installSnapshot 告诉Follower，需要从自己这里拉取snapshot然后在Follower上进行snapshot的load, 因为从 Replicator 内部记录的日志索引
//信息得出，当前的 Replicator 复制 Leader 的日志已经过慢了，
func (r *Replicator) installSnapshot() {

}

//sendEmptyEntries 发送一个空的LogEntry，用于心跳或者探测
func (r *Replicator) sendEmptyEntries(isHeartbeat bool, heartbeatClosure *AppendEntriesResponseClosure) {
	req := &raft.AppendEntriesRequest{}
	if r.fillCommonFields(req, r.nextIndex-1, isHeartbeat) {
		r.installSnapshot()
		return
	}
	defer func() {
		// 结束当前对复制者的信息发送，需要解放当前的 Replicator
		r.lock.Unlock()
	}()

	var heartbeatDone *AppendEntriesResponseClosure
	sendTime := time.Now()
	if isHeartbeat {
		r.heartbeatCounter++
		heartbeatDone = utils.IF(heartbeatClosure == nil, &AppendEntriesResponseClosure{RpcResponseClosure{F: func(resp proto.Message, status entity.Status) {
			r.onHeartbeatReqReturn(status, heartbeatClosure.Resp.(*raft.AppendEntriesResponse), sendTime)
		}}}, heartbeatClosure).(*AppendEntriesResponseClosure)

		r.heartbeatInFly = polerpc.NewMonoFuture(r.raftOperator.AppendEntries(r.options.peerId.GetEndpoint(), req,
			heartbeatDone))
	} else {
		req.Data = utils.EmptyBytes
		r.statInfo.runningState = AppendingEntries
		r.statInfo.firstLogIndex = r.nextIndex
		r.statInfo.lastLogIncluded = r.nextIndex - 1
		r.appendEntriesCounter++
		atomic.StoreInt32((*int32)(&r.state), int32(ReplicatorProbe))
		stateVersion := r.version
		reqSeq := r.getAndIncrementReqSeq()

		m := r.raftOperator.AppendEntries(r.options.peerId.GetEndpoint(), req,
			&AppendEntriesResponseClosure{RpcResponseClosure{
				F: func(resp proto.Message, status entity.Status) {
					r.onRpcReturn(RequestTypeForAppendEntries, status, req, resp, reqSeq, stateVersion, sendTime)
				},
			}})
		// 创建一个MonoFuture时，内部会自动做一个Subscribe(context.Context)的操作
		future := polerpc.NewMonoFuture(m)
		r.AddInFlights(RequestTypeForAppendEntries, r.nextIndex, 0, 0, reqSeq, future)
	}
	utils.RaftLog.Debug("node %s send HeartbeatRequest to %s term %s lastCommittedIndex %d",
		r.options.node.nodeID.GetDesc(), r.options.peerId.GetDesc(), r.options.term, req.CommittedIndex)
}

func (r *Replicator) onRpcReturn(reqType RequestType, status entity.Status, req, resp proto.Message,
	seq int64, stateVersion int32, rpcSendTime time.Time) {

}

func (r *Replicator) sendNextEntries(nextSendingIndex int64) {
	req := new(raft.AppendEntriesRequest)
	if !r.fillCommonFields(req, nextSendingIndex-1, false) {

	}
}

//sendHeartbeat
func (r *Replicator) sendHeartbeat(closure *AppendEntriesResponseClosure) {
	r.lock.Lock()
	r.sendEmptyEntries(true, nil)
}

//onVoteReqReturn
func (r *Replicator) onVoteReqReturn(resp *raft.RequestVoteResponse) {
}

//onHeartbeatReqReturn
func (r *Replicator) onHeartbeatReqReturn(status entity.Status, resp *raft.AppendEntriesResponse, sendTime time.Time) {
}

//onInstallSnapshotReqReturn
func (r *Replicator) onInstallSnapshotReqReturn(resp *raft.InstallSnapshotResponse) {

}

func (r *Replicator) getAndIncrementReqSeq() int64 {
	pre := r.reqSeq
	r.reqSeq++
	if r.reqSeq < 0 {
		r.reqSeq = 0
	}
	return pre
}

func (r *Replicator) fillCommonFields(req *raft.AppendEntriesRequest, prevLogIndex int64, isHeartbeat bool) bool {
	prevLogTerm := r.options.logMgn.GetTerm(prevLogIndex)
	if prevLogTerm == 0 && prevLogIndex != 0 {
		// 如果出现这种情况，则标示，至少在 prevLogIndex 以及 prevLogIndex 之前的 RaftLog 都被 purge 掉了
		if !isHeartbeat {
			if err := utils.RequireTrue(prevLogIndex < r.options.logMgn.GetFirstLogIndex(),
				"prevLogIndex must be less then current log manager first logIndex which logIndex have term"+
					" information"); err != nil {
				// 因为RaftLog被compacted了，因此该LogIndex对应的信息都不在了，无法填充相应的信息数据
				return false
			}
		} else {
			prevLogIndex = 0
		}
	}

	opt := r.options
	req.Term = opt.term
	req.GroupID = opt.groupID
	req.ServerID = opt.serverId.GetDesc()
	req.ServerID = opt.serverId.GetDesc()
	req.PeerID = opt.peerId.GetDesc()
	req.PrevLogIndex = prevLogIndex
	req.PrevLogTerm = prevLogTerm
	req.CommittedIndex = opt.ballotBox.lastCommittedIndex

	return true
}

func (r *Replicator) shutdown() {
	r.destroy = true
	r.futures.ForEach(func(index int, v interface{}) {
		v.(polerpc.Future).Cancel()
	})
}

func notifyOnCaughtUp(r *Replicator, errCode entity.RaftErrorCode) {

}

func notifyReplicatorStatusListener(r *Replicator, event ReplicatorEvent, st entity.Status) {

}

//onError 根据异常码 errCode 处理不同的逻辑
func onError(r *Replicator, errCode entity.RaftErrorCode) {
	switch errCode {
	case entity.ETIMEDOUT:
		// 触发心跳发送的逻辑
		r.lock.Unlock()
		utils.DefaultScheduler.Submit(func() {
			r.sendHeartbeat(nil)
		})
	case entity.EStop:
		// 停止某一个 Replicator
		defer r.shutdown()
		ele := r.inFlights.Front()
		for {
			val := ele.Next()
			if val == nil {
				break
			}
			inflight := val.Value.(*InFlight)
			if inflight != r.rpcInFly {
				inflight.future.Cancel()
			}
			if r.rpcInFly != nil {
				r.rpcInFly.future.Cancel()
				r.rpcInFly = nil
			}
			if r.heartbeatInFly != nil {
				r.heartbeatInFly.Cancel()
				r.heartbeatInFly = nil
			}
			if r.timeoutNowInFly != nil {
				r.timeoutNowInFly.Cancel()
				r.timeoutNowInFly = nil
			}
			r.futures.ForEach(func(index int, v interface{}) {
				v.(polerpc.Future).Cancel()
			})
			if r.waitId >= 0 {
				r.options.logMgn.RemoveWaiter(r.waitId)
			}
			notifyOnCaughtUp(r, errCode)
			ele = val.Next()
		}
	default:
		r.lock.Unlock()
		panic(fmt.Errorf("unknown error code for replicator: %d", errCode))
	}
}
