// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/heap"
	"container/list"
	"fmt"
	"math"
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
	RunningStateForIdle RunningState = iota
	RunningStateForBlocking
	RunningStateForAppendingEntries
	RunningStateForInstallingSnapshot
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
	runningState     RunningState // 当前 Replicator 的状态
	firstLogIndex    int64        // 记录上次发送给 Follower 的第一个 LogIndex
	lastLogIncluded  int64
	lastLogIndex     int64 // 记录上次发送给 Follower 的最后一个 LogIndex
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

func (ifl *InFlight) isSendingLogEntries() bool {
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
	version                int32 //TODO 该字段的用处
	seqGenerator           int64
	waitId                 int64
	reader                 SnapshotReader
	rpcInFly               *InFlight
	inFlights              *list.List    // <*InFlight>
	pendingResponses       *responseHeap // rpcResponse
	options                *replicatorOptions
	raftOptions            RaftOptions
	heartbeatInFly         polerpc.Future
	timeoutNowInFly        polerpc.Future
	blockFuture            polerpc.Future
	destroy                bool
	futures                *polerpc.ConcurrentSlice // polerpc.Future
}

type rpcResponse struct {
	status      entity.Status
	request     proto.Message
	response    proto.Message
	rpcSendTime time.Time
	seq         int64
	requestType RequestType
}

// IntHeap 是一个由整数组成的最小堆。
type responseHeap []rpcResponse

//Len 堆的大小
func (h responseHeap) Len() int {
	return len(h)
}

//Less 进行堆内元素比较
func (h responseHeap) Less(i, j int) bool {
	return h[i].seq < h[j].seq
}

//Swap 交换元素
func (h responseHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

//Push 元素押入小顶堆
func (h *responseHeap) Push(x interface{}) {
	*h = append(*h, x.(rpcResponse))
}

//Pop 弹出堆顶元素
func (h *responseHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func newReplicator(opts *replicatorOptions, raftOpts RaftOptions) *Replicator {
	return &Replicator{
		lock:             &sync.Mutex{},
		options:          opts,
		raftOptions:      raftOpts,
		nextIndex:        opts.logMgn.GetLastLogIndex() + 1,
		raftOperator:     opts.raftRpcOperator,
		futures:          &polerpc.ConcurrentSlice{},
		pendingResponses: &responseHeap{},
	}
}

func (r *Replicator) Start() (bool, error) {
	if ok, err := r.raftOperator.raftClient.CheckConnection(r.options.peerId.GetEndpoint()); !ok || err != nil {
		utils.RaftLog.Error("fail init sending channel to %s", r.options.peerId.GetDesc())
		return ok, err
	}
	r.lock.Lock()
	heap.Init(r.pendingResponses)
	notifyReplicatorStatusListener(r, ReplicatorCreatedEvent, entity.NewEmptyStatus())
	utils.RaftLog.Info("replicator=%#v@%s is started", r, r.options.peerId.GetDesc())
	r.lastRpcSendTimestamp = utils.GetCurrentTimeMs()
	r.startHeartbeat(utils.GetCurrentTimeMs())
	r.sendEmptyEntries(false, nil)
	return true, nil
}

func (r *Replicator) Stop() {

}

//addInFlights
func (r *Replicator) addInFlights(reqType RequestType, startIndex int64, cnt, size int32, seq int64,
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

//getNextSendIndex
func (r *Replicator) getNextSendIndex() int64 {
	if r.inFlights.Len() == 0 {
		return r.nextIndex
	}
	if int64(r.inFlights.Len()) > r.raftOptions.MaxReplicatorInflightMsgs {
		return -1
	}
	if r.rpcInFly != nil && r.rpcInFly.isSendingLogEntries() {
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

func (r *Replicator) stopTransferLeadership() bool {
	defer r.lock.Unlock()
	r.lock.Lock()
	r.timeoutNowIndex = 0
	return true
}

//installSnapshot 告诉Follower，需要从自己这里拉取snapshot然后在Follower上进行snapshot的load, 因为从 Replicator 内部记录的日志索引
//信息得出，当前的 Replicator 复制 Leader 的日志已经过慢了，
func (r *Replicator) installSnapshot() {

}

func (r *Replicator) sendTimeoutNow(unlockId, stopAfterFinish bool) {

}

//sendEmptyEntries 发送一个空的LogEntry，用于心跳或者探测
func (r *Replicator) sendEmptyEntries(isHeartbeat bool, heartbeatClosure *AppendEntriesResponseClosure) {
	req := &raft.AppendEntriesRequest{}
	if r.fillCommonFields(req, r.nextIndex-1, isHeartbeat) {
		r.installSnapshot()
		return
	}

	// 结束当前对复制者的信息发送，需要解放当前的 Replicator
	defer r.lock.Unlock()

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
		r.statInfo.runningState = RunningStateForAppendingEntries
		r.statInfo.firstLogIndex = r.nextIndex
		r.statInfo.lastLogIncluded = r.nextIndex - 1
		r.appendEntriesCounter++
		atomic.StoreInt32((*int32)(&r.state), int32(ReplicatorProbe))
		stateVersion := r.version
		reqSeq := r.getAndIncrementReqSeq()

		m := r.raftOperator.AppendEntries(r.options.peerId.GetEndpoint(), req,
			&AppendEntriesResponseClosure{RpcResponseClosure{
				F: func(resp proto.Message, status entity.Status) {
					r.onRpcReturned(RequestTypeForAppendEntries, status, req, resp, reqSeq, stateVersion, sendTime)
				},
			}})
		// 创建一个MonoFuture时，内部会自动做一个Subscribe(context.Context)的操作
		future := polerpc.NewMonoFuture(m)
		r.addInFlights(RequestTypeForAppendEntries, r.nextIndex, 0, 0, reqSeq, future)
	}
	utils.RaftLog.Debug("node %s send HeartbeatRequest to %s term %s lastCommittedIndex %d",
		r.options.node.nodeID.GetDesc(), r.options.peerId.GetDesc(), r.options.term, req.CommittedIndex)
}

func (r *Replicator) onRpcReturned(reqType RequestType, status entity.Status, req, resp proto.Message,
	seq int64, stateVersion int32, rpcSendTime time.Time) {
	startMs := time.Now()

	r.lock.Lock()

	//TODO 这个 version 的作用？
	if stateVersion != r.version {
		utils.RaftLog.Debug("Replicator %s ignored old version response %s, current version is %s, request is %#v\n, "+
			"and response is %#v\n, status is %s.", r, stateVersion, r.version, req, resp, status)
		r.lock.Unlock()
		return
	}

	holdingQueue := r.pendingResponses
	heap.Push(holdingQueue, rpcResponse{
		status:      status,
		request:     req,
		response:    resp,
		rpcSendTime: rpcSendTime,
		seq:         seq,
		requestType: reqType,
	})

	//TODO 应该是发现 Follower 许久没有做回应给Leader了，为了避免 Leader 节点对于 Replicator 保存的 RpcResponse 数量太多，因此这里需要做一个限制
	if int64(holdingQueue.Len()) > r.raftOptions.MaxReplicatorInflightMsgs {
		utils.RaftLog.Error("too many pending responses %d for replicator %s, maxReplicatorInflightMsgs=%d",
			holdingQueue.Len(), r.options.peerId.GetDesc(), r.raftOptions.MaxReplicatorInflightMsgs)
		r.resetInFlights()
		r.state = ReplicatorProbe
		r.sendEmptyEntries(false, nil)
		return
	}

	continueSendEntries := false

	defer func() {
		if continueSendEntries {
			r.sendEntries()
		}
	}()

	processed := 0
	for holdingQueue.Len() > 0 {
		resp := heap.Pop(holdingQueue).(rpcResponse)
		if resp.seq != r.requiredNextSeq {
			if processed > 0 {
				break
			} else {
				continueSendEntries = false
				r.lock.Unlock()
				return
			}
		}

		processed++
		inFlight := r.pollInFlight()
		if inFlight == nil {
			utils.RaftLog.Debug("ignore this response %#v because request's inFlight can't find", resp.response)
			continue
		}
		if inFlight.seq != resp.seq {
			r.resetInFlights()
			r.state = ReplicatorProbe
			continueSendEntries = false
			block(r, entity.ERequest, time.Now())
			return
		}

		isGoon := true

		dispatch := func() {
			defer func() {
				if continueSendEntries {
					r.getAndIncrementRequiredNextSeq()
				} else {
					isGoon = false
				}
			}()

			switch resp.requestType {
			case RequestTypeForAppendEntries:
				continueSendEntries = r.onAppendEntriesReturned(inFlight, status,
					resp.request.(*raft.AppendEntriesRequest),
					resp.response.(*raft.AppendEntriesResponse), rpcSendTime, startMs)
			case RequestTypeForSnapshot:
				continueSendEntries = r.onInstallSnapshotReqReturn(status, resp.request.(*raft.InstallSnapshotRequest),
					resp.response.(*raft.InstallSnapshotResponse))
			}
		}

		if dispatch(); !isGoon {
			break
		}
	}
}

//sendEntries
func (r *Replicator) sendEntries() {
	doUnlock := true
	defer func() {
		if doUnlock {
			r.lock.Unlock()
		}
	}()
	r.lock.Lock()

	prevSendIndex := int64(-1)

	for {
		nextSendingIndex := r.getNextSendIndex()
		if nextSendingIndex > prevSendIndex {
			if r.sendNextEntries(nextSendingIndex) {
				prevSendIndex = nextSendingIndex
			} else {
				doUnlock = false
				break
			}
		} else {
			break
		}
	}
}

//sendNextEntries
func (r *Replicator) sendNextEntries(nextSendingIndex int64) bool {
	req := new(raft.AppendEntriesRequest)
	// 先填充 AppendEntriesRequest 的基本信息数据，如果发现没办法满足填充的条件，就需要触发 Follower 进行一个 InstallSnapshot 的动作
	if !r.fillCommonFields(req, nextSendingIndex-1, false) {
		r.installSnapshot()
		return false
	}

	maxEntriesSize := int64(r.raftOptions.MaxEntriesSize)
	body := make([]byte, 0, 0)
	for i := int64(0); i < maxEntriesSize; i++ {
		emb := &raft.EntryMeta{}
		if ok, err := r.prepareEntry(body, nextSendingIndex+i, emb); err != nil || !ok {
			if err != nil {
				utils.RaftLog.Warn("prepare LogEntry to replicator failed : %s", err)
			}
			break
		}
		req.Entries = append(req.Entries, emb)
	}

	if len(body) == 0 {
		if nextSendingIndex < r.options.logMgn.GetFirstLogIndex() {
			r.installSnapshot()
			return false
		}
		// 当前没有可以同步给 Follower or Learner 的 LogEntry 了
		r.waitMoreEntries(nextSendingIndex)
		return false
	}

	req.Data = body

	utils.RaftLog.Debug("node %s send AppendEntriesRequest to %s term %s lastCommittedIndex %s prevLogIndex"+
		" %s prevLogTerm %s logIndex %s count %s", r.options.node.nodeID.GetDesc(), r.options.peerId.GetDesc(),
		r.options.term, req.CommittedIndex, req.PrevLogIndex, req.PrevLogTerm, nextSendingIndex, len(req.Entries))

	r.statInfo.runningState = RunningStateForAppendingEntries
	r.statInfo.firstLogIndex = req.PrevLogIndex + 1
	r.statInfo.lastLogIndex = req.PrevLogIndex + int64(len(req.Entries))

	version := r.version
	sendTimeMs := time.Now()
	seq := r.getAndIncrementReqSeq()

	m := r.raftOperator.AppendEntries(r.options.peerId.GetEndpoint(), req,
		&AppendEntriesResponseClosure{RpcResponseClosure{
			F: func(resp proto.Message, status entity.Status) {
				r.onRpcReturned(RequestTypeForAppendEntries, status, req, resp, seq, version, sendTimeMs)
			},
		}})
	// 创建一个MonoFuture时，内部会自动做一个Subscribe(context.Context)的操作
	future := polerpc.NewMonoFuture(m)
	r.addInFlights(RequestTypeForAppendEntries, nextSendingIndex, int32(len(req.Entries)), int32(len(body)), seq, future)
	return true
}

//sendHeartbeat
func (r *Replicator) sendHeartbeat(closure *AppendEntriesResponseClosure) {
	r.lock.Lock()
	r.sendEmptyEntries(true, closure)
}

//resetInFlights
func (r *Replicator) resetInFlights() {
	r.version++
	r.inFlights = list.New()
	r.pendingResponses = &responseHeap{}
	heap.Init(r.pendingResponses)
	rs := int64(math.Max(float64(r.reqSeq), float64(r.requiredNextSeq)))
	r.reqSeq = rs
	r.requiredNextSeq = rs
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			utils.RaftLog.Error("close snapshot reader has error : %s", err)
		}
	}
}

//onAppendEntriesReturned
func (r *Replicator) onAppendEntriesReturned(inFlight *InFlight, st entity.Status, req *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse, rpcSendTime, startMs time.Time) bool {

	return false
}

//onVoteReqReturn
func (r *Replicator) onVoteReqReturn(resp *raft.RequestVoteResponse) {
}

//onHeartbeatReqReturn
func (r *Replicator) onHeartbeatReqReturn(status entity.Status, resp *raft.AppendEntriesResponse, sendTime time.Time) {
}

//onInstallSnapshotReqReturn
func (r *Replicator) onInstallSnapshotReqReturn(st entity.Status, req *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse) bool {
	return false
}

func (r *Replicator) getAndIncrementReqSeq() int64 {
	pre := r.reqSeq
	r.reqSeq++
	if r.reqSeq < 0 {
		r.reqSeq = 0
	}
	return pre
}

func (r *Replicator) getAndIncrementRequiredNextSeq() int64 {
	pre := r.requiredNextSeq
	r.requiredNextSeq++
	if r.requiredNextSeq < 0 {
		r.requiredNextSeq = 0
	}
	return pre
}

type replicatorWait struct{}

func (rw *replicatorWait) OnNewLog(arg *Replicator, errCode entity.RaftErrorCode) {
	arg.continueSending(errCode)
}

func (r *Replicator) waitMoreEntries(nextWaitIndex int64) {
	utils.RaftLog.Debug("node {} waits more entries", r.options.node.nodeID.GetDesc())
	defer r.lock.Unlock()
	if r.waitId >= 0 {
		return
	}
	waitId := r.options.logMgn.Wait(nextWaitIndex-1, &replicatorWait{}, r)
	r.waitId = waitId
	r.statInfo.runningState = RunningStateForIdle
}

func (r *Replicator) continueSending(errCode entity.RaftErrorCode) {
	r.waitId = -1
	// 超时需要做一次 Prob 的探测动作
	if errCode == entity.ERaftTimedOut {
		r.blockFuture = nil
		r.sendEmptyEntries(false, nil)
	} else if errCode != entity.EStop {
		r.sendEntries()
	} else {
		utils.RaftLog.Warn("replicator {} stops sending entries.", r)
		r.lock.Unlock()
	}
}

func (r *Replicator) prepareEntry(body []byte, nextSendingIndex int64, emb *raft.EntryMeta) (bool, error) {
	if len(body) > int(r.raftOptions.MaxBodySize) {
		return false, nil
	}

	node := r.options.node
	entry := node.logManager.GetEntry(nextSendingIndex)
	if entry == nil {
		return false, nil
	}
	emb.Term = entry.LogID.GetTerm()
	emb.Checksum = entry.Checksum()
	emb.Type = entry.LogType
	if entry.Peers != nil {
		if err := utils.RequireTrue(len(entry.Peers) != 0, "empty peers at logIndex=%d",
			entry.LogID.GetIndex()); err != nil {
			return false, err
		}
		r.fillMetaPeers(emb, entry)
	} else {
		if err := utils.RequireTrue(entry.LogType != raft.EntryType_EntryTypeConfiguration,
			"empty peers but is ENTRY_TYPE_CONFIGURATION type at logIndex=%d",
			entry.LogID.GetIndex()); err != nil {
			return false, err
		}
	}
	emb.DataLen = int64(len(entry.Data))
	return false, nil
}

func (r *Replicator) fillMetaPeers(emb *raft.EntryMeta, entry *entity.LogEntry) {
	for _, peer := range entry.Peers {
		emb.Peers = append(emb.Peers, peer.GetDesc())
	}
	if entry.OldPeers != nil {
		for _, peer := range entry.OldPeers {
			emb.OldPeers = append(emb.OldPeers, peer.GetDesc())
		}
	}
	if entry.Learners != nil {
		for _, peer := range entry.Learners {
			emb.Learners = append(emb.Learners, peer.GetDesc())
		}
	}
	if entry.OldLearners != nil {
		for _, peer := range entry.OldLearners {
			emb.OldLearners = append(emb.OldLearners, peer.GetDesc())
		}
	}
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

//TODO 这一段逻辑的作用
//block
func block(r *Replicator, errCode entity.RaftErrorCode, startMs time.Time) {
	if r.blockFuture != nil {
		r.lock.Unlock()
		return
	}
	defer func() {
		if err := recover(); err != nil {
			utils.RaftLog.Error("fail to add timer : %s", err)
			r.sendEmptyEntries(false, nil)
		}
	}()
	utils.RaftLog.Debug("blocking {} for {} ms")
	dueTime := startMs.Add(time.Duration(r.options.dynamicHeartBeatTimeoutMs) * time.Millisecond)
	r.blockFuture = polerpc.DelaySchedule(func() {
		onBlockTimeout(r)
	}, dueTime.Sub(time.Now()))
	r.statInfo.runningState = RunningStateForBlocking
	r.lock.Unlock()
}

//onBlockTimeout 超时需要做的事情
func onBlockTimeout(r *Replicator) {
	polerpc.GoEmpty(func() {
		r.continueSending(entity.ERaftTimedOut)
	})
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
