package lraft

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"lraft/core"
	"lraft/entity"
	"lraft/rafterror"
	"lraft/storage"
	"lraft/utils"
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

func (cq *ClosureQueue) RestFirstIndex(firstIndex int64) {
	defer cq.lock.Unlock()
	cq.lock.Lock()

	utils.RequireTrue(cq.queue.Len() == 0, "queue is not empty.")
	cq.firstIndex = firstIndex
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

	Start() storage.SnapshotReader
}

type SaveSnapshotClosure interface {
	Closure

	Start(meta *core.SnapshotMeta) storage.SnapshotWriter
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

	utils.NewGoroutine(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-ticker.C:
				isOk := atomic.CompareAndSwapInt64(&rc.state, PENDING, TIMEOUT)
				if !isOk {
					return
				}
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
	rc.f(status, InvalidLogIndex, nil)
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
	ctx         context.Context
	errorWasSet bool
	status      entity.Status
	f           func(status entity.Status)
}

func (cuc *CatchUpClosure) Run(status entity.Status) {
	cuc.f(status)
}

func (cuc *CatchUpClosure) GetMaxMargin() int64 {
	return cuc.maxMargin
}

func (cuc *CatchUpClosure) SetMaxMargin(maxMargin int64) {
	cuc.maxMargin = maxMargin
}

func (cuc *CatchUpClosure) GetCtx() context.Context {
	return cuc.ctx
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
	Err rafterror.RaftError
	F	func(status entity.Status)
}

func (oec *OnErrorClosure) Run(status entity.Status) {

}

type RpcResponseClosure struct {
	Resp	proto.Message
	F	func(status entity.Status)
}

func (rrc *RpcResponseClosure) Run(status entity.Status)  {
	rrc.F(status)
}
