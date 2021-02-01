// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	proto2 "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/utils"
)

type Closure interface {
	Run(status entity.Status)
}

type ClosureQueue struct {
	lock       sync.Mutex
	firstIndex int64
	queue      list.List
}

func (cq *ClosureQueue) GetFirstIndex() int64 {
	return cq.firstIndex
}

func (cq *ClosureQueue) GetQueue() list.List {
	return cq.queue
}

func (cq *ClosureQueue) Clear() {
	cq.lock.Lock()
	t := cq.queue
	cq.queue = list.List{}
	cq.firstIndex = 0
	cq.lock.Unlock()

	status := entity.NewStatus(entity.EPERM, "Leader stepped down")

	e := t.Front()
	for e != nil {
		done := e.Value.(Closure)
		done.Run(status)
	}
}

func (cq *ClosureQueue) IsEmpty() bool {
	return cq.queue.Len() == 0
}

func (cq *ClosureQueue) RestFirstIndex(firstIndex int64) bool {
	defer cq.lock.Unlock()
	cq.lock.Lock()

	if err := utils.RequireTrue(cq.queue.Len() == 0, "queue is not empty."); err != nil {
		return false
	}
	cq.firstIndex = firstIndex
	return true
}

func (cq *ClosureQueue) AppendPendingClosure(closure Closure) {
	defer cq.lock.Unlock()
	cq.lock.Lock()

	cq.queue.PushBack(closure)
}

func (cq *ClosureQueue) PopClosureUntil(endIndex int64, closures []Closure, tasks []TaskClosure) int64 {
	closures = make([]Closure, 0)
	if len(tasks) != 0 {
		tasks = make([]TaskClosure, 0)
	}

	defer cq.lock.Unlock()
	cq.lock.Lock()

	qSize := int64(cq.queue.Len())
	if qSize == 0 || endIndex < cq.firstIndex {
		return endIndex + 1
	}
	if endIndex > cq.firstIndex+qSize-1 {
		// TODO Log
		return -1
	}
	outFirstIndex := cq.firstIndex
	for i := outFirstIndex; i <= endIndex; i++ {
		e := cq.queue.Front()
		cq.queue.Remove(e)
		switch t := e.Value.(type) {
		case TaskClosure:
			if tasks != nil {
				tasks = append(tasks, t)
			}
		case Closure:
			// do nothing
		}
		closures = append(closures, e.Value.(Closure))
	}
	cq.firstIndex = endIndex + 1
	return outFirstIndex
}

type SynchronizedClosure struct {
	latch  *sync.WaitGroup
	status *entity.Status
	count  int
}

func NewSynchronizedClosure(cnt int) *SynchronizedClosure {
	latch := &sync.WaitGroup{}
	latch.Add(cnt)

	return &SynchronizedClosure{
		latch: latch,
		count: cnt,
	}
}

func (sc *SynchronizedClosure) GetStatus() entity.Status {
	return *sc.status
}

func (sc *SynchronizedClosure) Run(status entity.Status) {
	sc.status = &status
	sc.latch.Done()
}

func (sc *SynchronizedClosure) Await() entity.Status {
	sc.latch.Wait()
	return *sc.status
}

func (sc *SynchronizedClosure) Rest() {
	sc.status = nil
	l := sync.WaitGroup{}
	l.Add(sc.count)
	sc.latch = &l
}

type TaskClosure interface {
	Closure

	OnCommitted()
}

type LoadSnapshotClosure interface {
	Closure

	Start() SnapshotReader
}

type SaveSnapshotClosure interface {
	Closure

	Start(meta *proto2.SnapshotMeta) SnapshotWriter
}

const (
	PENDING = iota
	COMPLETE
	TIMEOUT

	InvalidLogIndex = -1
)

type ReadIndexClosure struct {
	index          int64
	requestContext []byte
	state          int64
	f              func(status entity.Status, index int64, reqCtx []byte)
}

func NewReadIndexClosure(f func(status entity.Status, index int64, reqCtx []byte), timeout time.Duration) *ReadIndexClosure {
	rc := &ReadIndexClosure{
		index:          0,
		requestContext: nil,
		state:          PENDING,
		f:              f,
	}

	ticker := time.NewTicker(timeout)

	polerpc.Go(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-ticker.C:
				isOk := atomic.CompareAndSwapInt64(&rc.state, PENDING, TIMEOUT)
				if !isOk {
					return
				}
				rc.SetResult(InvalidLogIndex, nil)
				rc.runUserCallback(entity.NewStatus(entity.ETIMEDOUT, "read-index request timeout"))
				ticker.Stop()
			}
		}
	})

	return rc
}

func (rc *ReadIndexClosure) runUserCallback(status entity.Status) {
	defer func() {
		if err := recover(); err != nil {
			//TODO error log
		}
	}()
	rc.f(status, rc.index, rc.requestContext)
}

func (rc *ReadIndexClosure) SetResult(index int64, reqCtx []byte) {
	rc.index = index
	rc.requestContext = reqCtx
}

func (rc *ReadIndexClosure) Run(status entity.Status) {
	defer func() {
		if err := recover(); err != nil {
			//TODO error log
		}
	}()

	isOk := atomic.CompareAndSwapInt64(&rc.state, PENDING, COMPLETE)
	if !isOk {
		//TODO Log
		return
	}
	rc.runUserCallback(status)
}

type CatchUpClosure struct {
	maxMargin   int64
	future      utils.Future
	errorWasSet bool
	status      entity.Status
	F           func(status entity.Status)
}

func (cuc *CatchUpClosure) Run(status entity.Status) {
	cuc.F(status)
}

func (cuc *CatchUpClosure) GetMaxMargin() int64 {
	return cuc.maxMargin
}

func (cuc *CatchUpClosure) SetMaxMargin(maxMargin int64) {
	cuc.maxMargin = maxMargin
}

func (cuc *CatchUpClosure) GetFuture() utils.Future {
	return cuc.future
}

func (cuc *CatchUpClosure) IsErrorWasSet() bool {
	return cuc.errorWasSet
}

func (cuc *CatchUpClosure) SetErrorWasSet(errorWasSet bool) {
	cuc.errorWasSet = errorWasSet
}

type StableClosure struct {
	FirstLogIndex int64
	Entries       []*entity.LogEntry
	NEntries      int32
	f             func(status entity.Status)
}

func NewStableClosure(entries []*entity.LogEntry, f func(status entity.Status)) *StableClosure {
	return &StableClosure{
		Entries: entries,
		f:       f,
	}
}

func (sc *StableClosure) Run(status entity.Status) {
	sc.f(status)
}

type OnErrorClosure struct {
	Err entity.RaftError
	F   func(status entity.Status)
}

func (oec *OnErrorClosure) Run(status entity.Status) {

}

const (
	RpcPending int32 = iota
	RpcRespond
)

type RpcResponseClosure struct {
	Resp proto.Message
	F    func(resp proto.Message, status entity.Status)
}

func (rrc *RpcResponseClosure) Run(status entity.Status) {
	rrc.F(rrc.Resp, status)
}

type RpcRequestClosure struct {
	state       int32
	rpcCtx      *rpc.RpcContext
	defaultResp *polerpc.ServerResponse
	F           func(status entity.Status)
}

func NewRpcRequestClosure(rpcCtx *rpc.RpcContext) *RpcRequestClosure {
	return &RpcRequestClosure{
		state:       RpcPending,
		rpcCtx:      rpcCtx,
		defaultResp: nil,
	}
}

func NewRpcRequestClosureWithDefaultResp(rpcCtx *rpc.RpcContext, defaultResp *polerpc.ServerResponse) *RpcRequestClosure {
	return &RpcRequestClosure{
		state:       RpcPending,
		rpcCtx:      rpcCtx,
		defaultResp: defaultResp,
	}
}

func (rrc *RpcRequestClosure) GetRpcCtx() *rpc.RpcContext {
	return rrc.rpcCtx
}

func (rrc *RpcRequestClosure) SendResponse(msg *polerpc.ServerResponse) {
	if atomic.CompareAndSwapInt32(&rrc.state, RpcPending, RpcRespond) {
		rrc.rpcCtx.SendMsg(msg)
	}
}

func (rrc *RpcRequestClosure) Run(status entity.Status) {

	errResp := &proto2.ErrorResponse{
		ErrorCode: int32(status.GetCode()),
		ErrorMsg:  status.GetMsg(),
	}

	a, err := ptypes.MarshalAny(errResp)
	if err != nil {
		panic(err)
	}

	rrc.SendResponse(&polerpc.ServerResponse{
		FunName: rpc.CommonRpcErrorCommand,
		Body:    a,
	})
}

type OnPreVoteRpcDone struct {
	RpcResponseClosure
	PeerId    entity.PeerId
	Term      int64
	StartTime time.Time
	Req       *proto2.RequestVoteRequest
}

type OnRequestVoteRpcDone struct {
	RpcResponseClosure
	PeerId    entity.PeerId
	Term      int64
	StartTime time.Time
	node      *nodeImpl
	Req       *proto2.RequestVoteRequest
}

type AppendEntriesResponseClosure struct {
	RpcResponseClosure
}

type RequestVoteResponseClosure struct {
	RpcResponseClosure
}

type InstallSnapshotResponseClosure struct {
	RpcResponseClosure
}

type TimeoutNowResponseClosure struct {
	RpcResponseClosure
}
