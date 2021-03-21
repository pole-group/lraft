// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

type ReadIndexStatus struct {
	States []*ReadIndexState
	Req    *raft.ReadIndexRequest
	Index  int64
}

func (ris *ReadIndexStatus) IsApplied(appliedIndex int64) bool {
	return appliedIndex >= ris.Index
}

type ReadIndexState struct {
	Index     int64
	reqCtx    []byte
	Done      *ReadIndexClosure
	startTime time.Time
}

func NewReadIndexState(reqCtx []byte, done *ReadIndexClosure, startTime time.Time) *ReadIndexState {
	return &ReadIndexState{
		Index:     -1,
		reqCtx:    reqCtx,
		Done:      done,
		startTime: startTime,
	}
}

type ReadOnlyOperator struct {
	rwLock              sync.RWMutex
	fsmCaller           FSMCaller
	err                 *entity.RaftError
	raftOpt             RaftOption
	node                *nodeImpl
	replicatorGroup     *ReplicatorGroup
	raftClientOperator  *RaftClientOperator
	pendingNotifyStatus *utils.TreeMap // <Long, List<*ReadIndexStatus>>
	shutdownWait        *sync.WaitGroup
}

func (rop *ReadOnlyOperator) addRequest(reqCtx []byte, done *ReadIndexClosure) {
	if rop.shutdownWait != nil {
		done.Run(entity.NewStatus(entity.ENodeShutdown, "node was stopped"))
		return
	}
	retryCnt := 3
	for i := 0; i < retryCnt; i++ {
		success, err := utils.PublishEventNonBlock(&ReadIndexEvent{
			reqCtx:    reqCtx,
			done:      done,
			startTime: time.Now(),
		})
		if err != nil {
			done.Run(entity.NewStatus(entity.EInternal, err.Error()))
			return
		}
		if success {
			return
		}
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	done.Run(entity.NewStatus(entity.Ebusy, "node is busy, hash too many read-only request"))
}

//OnApplied 监听当前状态机已经将哪一些 core.LogEntry 给 apply 成功了, 这里传入了当前最新的, appliedLogIndex
func (rop *ReadOnlyOperator) OnApplied(lastAppliedLogIndex int64) {
	notifyList := list.New()

	defer func() {
		rop.rwLock.Unlock()
		if notifyList.Len() != 0 {
			ele := notifyList.Front()
			for {
				if ele == nil {
					break
				}
				status := ele.Value.(*ReadIndexStatus)
				rop.notifySuccess(*status)
			}
		}
	}()

	//因为涉及 TreeMap 的数据查询以及删除，因此这里需要加上 WriteLock
	rop.rwLock.Lock()
	if rop.pendingNotifyStatus.IsEmpty() {
		return
	}
	rop.pendingNotifyStatus.RangeLessThan(lastAppliedLogIndex, func(k, v interface{}) {
		statusList := v.(*list.List)
		ele := statusList.Front()
		for {
			if ele == nil {
				break
			}
			status := ele.Value.(*ReadIndexStatus)
			notifyList.PushBack(ele.Value)
			ele = ele.Next()
			rop.pendingNotifyStatus.RemoveKey(status.Index)
		}
	})

	if rop.err != nil {
		rop.resetPendingStatusError(rop.err.Status)
	}
}

func (rop *ReadOnlyOperator) notifySuccess(status ReadIndexStatus) {
	nowTime := time.Now()
	states := status.States
	for _, state := range states {
		done := state.Done
		if done != nil {
			//TODO metrics 记录每一个 read-index 从请求开始到可以处理的时间信息
			utils.RaftLog.Debug("read-index : %s", nowTime.Sub(state.startTime))
			done.SetResult(state.Index, state.reqCtx)
			done.Run(entity.StatusOK())
		}
	}
}

func (rop *ReadOnlyOperator) resetPendingStatusError(st entity.Status) {
	defer rop.rwLock.Unlock()
	rop.rwLock.Lock()
	rop.pendingNotifyStatus.RangeEntry(func(k, v interface{}) {
		status := v.(*ReadIndexStatus)
		nowTime := time.Now()
		states := status.States
		for _, state := range states {
			done := state.Done
			if done != nil {
				//TODO metrics 记录每一个 read-index 从请求开始到可以处理的时间信息
				utils.RaftLog.Debug("read-index : %s", nowTime.Sub(state.startTime))
				done.Run(st)
			}
		}
	})
	rop.pendingNotifyStatus.Clear()
}

func (rop *ReadOnlyOperator) handleReadIndexRequest(req *raft.ReadIndexRequest, done *ReadIndexResponseClosure) {
	done.readIndexOperator = rop
	startTime := time.Now()

	defer func() {
		if err := recover(); err != nil {
			utils.RaftLog.Error("handle read-index occur error : %s", err)
		}

		rop.node.lock.RUnlock()
		utils.RaftLog.Debug("handle-read-index %s", time.Now().Sub(startTime))
		// TODO metrics 信息记录
	}()

	rop.node.lock.RLock()

	switch rop.node.state {
	case StateLeader:
		rop.readLeader(req, done)
	case StateFollower:
		rop.readFollower(req, done)
	case StateTransferring:
		done.Run(entity.NewStatus(entity.Ebusy, "is transferring leadership"))
	default:
		done.Run(entity.NewStatus(entity.EPerm, fmt.Sprintf("invalid state for read-index : %s.", rop.node.state.GetName())))
	}
}

//readLeader
func (rop *ReadOnlyOperator) readLeader(req *raft.ReadIndexRequest, done *ReadIndexResponseClosure) {
	n := rop.node
	logMgn := n.logManager
	quorum := n.GetQuorum()
	if quorum <= 1 {
		done.Resp = &raft.ReadIndexResponse{
			Index:   n.ballotBox.lastCommittedIndex,
			Success: true,
		}
		done.Run(entity.StatusOK())
		return
	}

	resp := &raft.ReadIndexResponse{}

	lastCommittedIndex := n.ballotBox.lastCommittedIndex
	if logMgn.GetTerm(lastCommittedIndex) != n.currTerm {
		done.Run(entity.NewStatus(entity.Eagain,
			fmt.Sprintf("ReadIndex request rejected because leader has not committed any log entry at its term, "+
				"logIndex=%d, currTerm=%d.", lastCommittedIndex, n.currTerm)))
		return
	}

	resp.Index = lastCommittedIndex
	if req.PeerID != "" {
		peer := entity.PeerId{}
		peer.Parse(req.PeerID)
		if !n.conf.ContainPeer(peer) && !n.conf.ContainLearner(peer) {
			done.Run(entity.NewStatus(entity.EPerm, fmt.Sprintf("Peer %s is not in current configuration: %#v.",
				req.PeerID, n.conf)))
			return
		}
	} else {
		// 如果请求没有携带 PeerId 的信息，认为该请求是错误的，有问题的
		done.Run(entity.NewStatus(entity.ERequest, fmt.Sprintf("ReadIndexRequest must be have Peer info")))
		return
	}

	readOnlyOpt := n.raftOptions.ReadOnlyOpt
	if readOnlyOpt == ReadOnlyLeaseBased && !n.leaderLeaseIsValid() {
		readOnlyOpt = ReadOnlySafe
	}

	switch readOnlyOpt {
	case ReadOnlyLeaseBased:
		// 根据时间任期处理的
		resp.Success = true
		done.Resp = resp
		done.Run(entity.StatusOK())
	case ReadOnlySafe:
		// 需要和 follower 沟通处理
		peers := n.conf.GetConf().ListPeers()
		if err := utils.RequireTrue(peers != nil && len(peers) != 0, "empty peers"); err != nil {
			done.Run(entity.NewStatus(entity.EInternal, err.Error()))
			return
		}
		heartbeatDone := NewReadIndexHeartbeatResponseClosure(done, resp, int32(quorum), int32(len(peers)))
		for _, peer := range peers {
			if peer.Equal(n.serverID) {
				continue
			}
			n.replicatorGroup.sendHeartbeat(peer, &heartbeatDone.AppendEntriesResponseClosure)
		}
	}
}

//readFollower
func (rop *ReadOnlyOperator) readFollower(req *raft.ReadIndexRequest, done *ReadIndexResponseClosure) {
	n := rop.node
	if n.leaderID.IsEmpty() {
		done.Run(entity.NewStatus(entity.EPerm, fmt.Sprintf("no leader ad term : %d", n.currTerm)))
		return
	}
	req.PeerID = n.leaderID.GetDesc()

	rop.raftClientOperator.ReadIndex(n.leaderID.GetEndpoint(), req, done)
}

type ReadIndexEvent struct {
	reqCtx       []byte
	done         *ReadIndexClosure
	shutdownWait *sync.WaitGroup
	startTime    time.Time
}

// Topic of the event
func (re *ReadIndexEvent) Name() string {
	return "ReadIndexEvent"
}

// The sequence number of the event
func (re *ReadIndexEvent) Sequence() int64 {
	return time.Now().Unix()
}

type ReadIndexEventSubscriber struct {
	rop        *ReadOnlyOperator
	batchEvent []*ReadIndexEvent
	batchSize  int32
	cursor     int32
}

func (res *ReadIndexEventSubscriber) OnEvent(event utils.Event, endOfBatch bool) {
	e := event.(*ReadIndexEvent)
	if e.shutdownWait != nil {
		res.execReadIndexEvent(res.batchEvent)
		res.batchEvent = make([]*ReadIndexEvent, 0, 0)
		e.shutdownWait.Done()
		return
	}
	res.batchEvent[res.cursor] = e
	res.cursor++
	if res.cursor >= res.batchSize || endOfBatch {
		res.execReadIndexEvent(res.batchEvent)
		res.batchEvent = make([]*ReadIndexEvent, res.batchSize, res.batchSize)
		res.cursor = 0
	}
}

func (res *ReadIndexEventSubscriber) execReadIndexEvent(events []*ReadIndexEvent) {
	rop := res.rop
	if len(events) == 0 {
		return
	}
	req := &raft.ReadIndexRequest{
		GroupID:  rop.node.GetGroupID(),
		ServerID: rop.node.serverID.GetDesc(),
		Entries:  make([][]byte, len(events), len(events)),
		PeerID:   "",
	}

	states := make([]*ReadIndexState, len(events), len(events))

	for i, event := range events {
		req.Entries[i] = event.reqCtx
		states[i] = NewReadIndexState(event.reqCtx, event.done, event.startTime)
	}

	// 交由 node 去处理 readIndex 的请求事件
	rop.handleReadIndexRequest(req, NewReadIndexResponseClosure(states, req))
}

func (res *ReadIndexEventSubscriber) IgnoreExpireEvent() bool {
	return false
}

func (res *ReadIndexEventSubscriber) SubscribeType() utils.Event {
	return &ReadIndexEvent{}
}

type ReadIndexResponseClosure struct {
	RpcResponseClosure
	states            []*ReadIndexState
	req               *raft.ReadIndexRequest
	readIndexOperator *ReadOnlyOperator
}

func NewReadIndexResponseClosure(states []*ReadIndexState, req *raft.ReadIndexRequest) *ReadIndexResponseClosure {
	return &ReadIndexResponseClosure{
		states: states,
		req:    req,
	}
}

func (rrc *ReadIndexResponseClosure) Run(status entity.Status) {
	if !status.IsOK() {
		rrc.notifyFail(status)
		return
	}
	resp := rrc.Resp.(*raft.ReadIndexResponse)
	if !resp.Success {
		rrc.notifyFail(entity.NewStatus(entity.Unknown, "Fail to run ReadIndex task, maybe the leader stepped down."))
		return
	}
	readIndexStatus := ReadIndexStatus{
		States: rrc.states,
		Req:    rrc.req,
		Index:  resp.GetIndex(),
	}

	for _, state := range rrc.states {
		state.Index = resp.Index
	}

	doUnlock := true
	defer func() {
		if doUnlock {
			rrc.readIndexOperator.rwLock.Unlock()
		}
	}()

	rrc.readIndexOperator.rwLock.Lock()
	if readIndexStatus.IsApplied(rrc.readIndexOperator.fsmCaller.GetLastAppliedIndex()) {
		rrc.readIndexOperator.rwLock.Unlock()
		doUnlock = false
		rrc.readIndexOperator.notifySuccess(readIndexStatus)
		return
	} else {
		rrc.readIndexOperator.pendingNotifyStatus.ComputeIfAbsent(readIndexStatus.Index, func() interface{} {
			return list.New()
		}).(*list.List).PushBack(readIndexStatus)
	}

}

func (rrc *ReadIndexResponseClosure) notifyFail(status entity.Status) {
	nowT := time.Now()
	for _, readIndexStatus := range rrc.states {
		// TODO 需要使用 metrics 组件
		utils.RaftLog.Debug("read-index %s", nowT.Sub(readIndexStatus.startTime))
		done := readIndexStatus.Done
		if done != nil {
			done.SetResult(InvalidLogIndex, readIndexStatus.reqCtx)
			done.Run(status)
		}
	}
}

type readIndexHeartbeatResponseClosure struct {
	AppendEntriesResponseClosure
	readIndexResp      *raft.ReadIndexResponse
	closure            *ReadIndexResponseClosure
	quorum             int32
	failPeersThreshold int32
	ackSuccess         int32
	ackFailures        int32
	isDone             bool
}

//NewReadIndexHeartbeatResponseClosure readIndexResp 不涉及网络传输，根据从 Leader 返回的 AppendEntriesResponse 信息决定 readIndexResp
//的内容是什么
func NewReadIndexHeartbeatResponseClosure(done *ReadIndexResponseClosure, readIndexResp *raft.ReadIndexResponse, quorum, peerSize int32) *readIndexHeartbeatResponseClosure {
	rhc := &readIndexHeartbeatResponseClosure{
		readIndexResp:      readIndexResp,
		closure:            done,
		quorum:             quorum,
		failPeersThreshold: utils.IF(peerSize%2 == 0, quorum-1, quorum).(int32),
		ackSuccess:         0,
		ackFailures:        0,
		isDone:             false,
	}

	rhc.AppendEntriesResponseClosure = AppendEntriesResponseClosure{
		RpcResponseClosure{
			F: func(resp proto.Message, status entity.Status) {
				if rhc.isDone {
					return
				}
				if status.IsOK() && resp.(*raft.AppendEntriesResponse).Success {
					rhc.ackSuccess++
				} else {
					rhc.ackFailures++
				}
				_, err := utils.RequireNonNil(rhc.readIndexResp, "ReadIndexResponse")
				if err != nil {
					rhc.closure.Run(entity.NewStatus(entity.EInternal, err.Error()))
					return
				}
				if rhc.ackSuccess+1 >= rhc.quorum {
					rhc.readIndexResp.Success = true
				} else if rhc.ackFailures >= rhc.failPeersThreshold {
					rhc.readIndexResp.Success = false
				}
				rhc.closure.Resp = rhc.readIndexResp
				rhc.closure.Run(entity.StatusOK())
				rhc.isDone = true
			},
		},
	}

	return rhc
}

func (rhc *readIndexHeartbeatResponseClosure) Run(status entity.Status) {
	rhc.RpcResponseClosure.Run(status)
}
