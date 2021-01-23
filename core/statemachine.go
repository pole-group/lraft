// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

type TaskType int

const (
	TaskIdle TaskType = iota
	TaskCommitted
	TaskSnapshotSave
	TaskSnapshotLoad
	TaskLeaderStop
	TaskLeaderStart
	TaskStartFollowing
	TaskStopFollowing
	TaskShutdown
	TaskFlush
	TaskError
)

const (
	ErrSetLastCommittedIndex = "node changes to leader, pendingIndex=%d, param lastCommittedIndex=%d"
	ErrAppendPendingTask     = "fail to appendingTask, pendingIndex=%d"
)

type Iterator interface {
	GetData() []byte

	GetIndex() int64

	GetTerm() int64

	Done() Closure

	SetErrorAndRollback(nTail int64, st entity.Status) error
}

type StateMachine interface {
	OnApply(iterator Iterator)

	OnShutdown()

	OnSnapshotSave(writer SnapshotWriter, done Closure)

	OnSnapshotLoad(reader SnapshotReader) bool

	OnLeaderStart(term int64)

	OnLeaderStop(status entity.Status)

	OnError(e entity.RaftError)

	OnConfigurationCommitted(conf *entity.Configuration)

	OnStopFollowing(ctx entity.LeaderChangeContext)

	OnStartFollowing(ctx entity.LeaderChangeContext)
}

const (
	ErrRollbackMsg = "StateMachine meet critical error when applying one or more tasks since index=%d, %s"
)

type IteratorImpl struct {
	fsm               StateMachine
	logManager        LogManager
	closures          []Closure
	firstClosureIndex int64
	currentIndex      int64
	committedIndex    int64
	currEntry         *entity.LogEntry
	applyingIndex     int64
	err               *entity.RaftError
}

func (iti *IteratorImpl) Entry() *entity.LogEntry {
	return iti.currEntry
}

func (iti *IteratorImpl) GetError() *entity.RaftError {
	return iti.err
}

func (iti *IteratorImpl) IsGood() bool {
	return iti.currentIndex <= iti.committedIndex && iti.err == nil
}

func (iti *IteratorImpl) HasError() bool {
	return iti.err != nil
}

func (iti *IteratorImpl) Next() {
	defer func() {
		if err := recover(); err != nil {
			iti.err = iti.GetOrCreateError()
			iti.err.ErrType = raft.ErrorType_ErrorTypeLog
			iti.err.Status.SetError(entity.EINVAL, err.(error).Error())
		}
	}()

	iti.currEntry = nil
	if iti.currentIndex <= iti.committedIndex && iti.currentIndex+1 <= iti.committedIndex {
		iti.currentIndex++
		iti.currEntry = iti.logManager.GetEntry(iti.currentIndex)
		if iti.currEntry == nil {
			iti.err = iti.GetOrCreateError()
			iti.err.ErrType = raft.ErrorType_ErrorTypeLog
			iti.err.Status.SetError(-1, "Fail to get entry at index=%d while committed_index=%d", iti.currentIndex, iti.committedIndex)
		}
		atomic.StoreInt64(&iti.applyingIndex, iti.currentIndex)
	}
}

func (iti *IteratorImpl) RunTenRestClosureWithError() {
	for i := int64(math.Max(float64(iti.currentIndex), float64(iti.firstClosureIndex))); i < iti.committedIndex; i++ {
		done := iti.closures[i-iti.firstClosureIndex]
		if done != nil {
			if _, err := utils.RequireNonNil(iti.err, "error"); err != nil {
				done.Run(entity.NewStatus(entity.EINVAL, "impossible run into here"))
				continue
			}
			done.Run(iti.err.Status)
		}
	}
}

func (iti *IteratorImpl) SetErrorAndRollback(nTail int64, st *entity.Status) error {
	if err := utils.RequireTrue(nTail > 0, "Invalid nTail=%d", nTail); err != nil {
		// TODO 有异常需要处理
		return err
	}
	// TODO 回滚个数需要研究
	// 如果当前的没有 LogEntry 或者不是用户态的数据，则就会会滚对应的 LogEntry 个数，否则需要多会滚一个，因此此时 LogEntry 已经向前推进了
	if iti.currEntry == nil || iti.currEntry.LogType != raft.EntryType_EntryTypeData {
		iti.currentIndex -= nTail
	} else {
		iti.currentIndex -= nTail - 1
	}
	iti.currEntry = nil
	iti.err = iti.GetOrCreateError()
	iti.err.ErrType = raft.ErrorType_ErrorTypeStateMachine
	iti.err.Status.SetError(entity.EStateMachine, ErrRollbackMsg, iti.currentIndex, utils.IF(st != nil, st, "none"))
	return nil
}

func (iti *IteratorImpl) GetOrCreateError() *entity.RaftError {
	if iti.err == nil {
		iti.err = &entity.RaftError{
			Status: entity.NewEmptyStatus(),
		}
	}
	return iti.err
}

func (iti *IteratorImpl) GetIndex() int64 {
	return iti.currentIndex
}

func (iti *IteratorImpl) Done() Closure {
	if iti.currentIndex < iti.firstClosureIndex {
		return nil
	}
	return iti.closures[iti.currentIndex-iti.firstClosureIndex]
}

type IteratorWrapper struct {
	impl *IteratorImpl
}

func NewIteratorWrapper(impl *IteratorImpl) *IteratorWrapper {
	return &IteratorWrapper{
		impl: impl,
	}
}

//HasNext 只判断当前是否可以进行向前推进一个LogEntry以及当前是否是用户态的数据信息
func (iw *IteratorWrapper) HasNext() bool {
	return iw.impl.IsGood() && iw.impl.currEntry.LogType == raft.EntryType_EntryTypeData
}

func (iw *IteratorWrapper) Next() []byte {
	data := iw.GetData()
	if iw.HasNext() {
		iw.impl.Next()
	}
	return data
}

func (iw *IteratorWrapper) GetIndex() int64 {
	return iw.impl.GetIndex()
}

func (iw *IteratorWrapper) GetTerm() int64 {
	return iw.impl.Entry().LogID.GetTerm()
}

func (iw *IteratorWrapper) Done() Closure {
	return iw.impl.Done()
}

func (iw *IteratorWrapper) SetErrorAndRollback(nTail int64, st entity.Status) error {
	return iw.impl.SetErrorAndRollback(nTail, &st)
}

func (iw *IteratorWrapper) GetData() []byte {
	entry := iw.impl.Entry()
	return utils.IF(entry != nil, entry.Data, nil).([]byte)
}

type LastAppliedLogIndexListener interface {
	//OnApplied 监听当前状态机已经将哪一些 core.LogEntry 给 apply 成功了, 这里传入了当前最新的, appliedLogIndex
	OnApplied(lastAppliedLogIndex int64)
}

type ApplyTask struct {
	TType               TaskType
	CommittedIndex      int64
	Term                int64
	Status              *entity.Status
	LeaderChangeContext *entity.LeaderChangeContext
	Done                Closure
	Latch               *sync.WaitGroup
}

func (at *ApplyTask) Reset() {
	at.TType = -1
	at.CommittedIndex = 0
	at.Term = -1
	at.Status = nil
	at.LeaderChangeContext = nil
	at.Done = nil
	at.Latch = nil
}

func (at *ApplyTask) Name() string {
	return "ApplyTask"
}

func (at *ApplyTask) Sequence() int64 {
	return time.Now().Unix()
}

type applyTaskHandler struct {
	maxCommittedIndex int64
	fsmImpl           *FSMCallerImpl
}

func (ath *applyTaskHandler) OnEvent(event utils.Event, endOfBatch bool) {
	applyTask := event.(*ApplyTask)
	ath.maxCommittedIndex = ath.fsmImpl.runApplyTask(applyTask, ath.maxCommittedIndex, endOfBatch)
	applyTask.Reset()
	ath.fsmImpl.applyTaskPool.Put(event)
}

func (ath *applyTaskHandler) SubscribeType() utils.Event {
	return &ApplyTask{}
}

func (ath *applyTaskHandler) IgnoreExpireEvent() bool {
	return false
}

type FSMCaller interface {
	AddLastAppliedLogIndexListener(listener LastAppliedLogIndexListener)

	OnCommitted(committedIndex int64) bool

	OnSnapshotLoad(done LoadSnapshotClosure) bool

	OnSnapshotSave(done SaveSnapshotClosure) bool

	OnLeaderStop(status entity.Status) bool

	OnLeaderStart(term int64) bool

	OnStartFollowing(context entity.LeaderChangeContext) bool

	OnStopFollowing(context entity.LeaderChangeContext) bool

	OnError(err entity.RaftError) bool

	GetLastAppliedIndex() int64

	Join()
}

type FSMCallerImpl struct {
	logManager                   LogManager
	fsm                          StateMachine
	closureQueue                 *ClosureQueue
	lastAppliedIndex             int64
	lastAppliedTerm              int64
	afterShutdown                Closure
	node                         *nodeImpl
	currTask                     TaskType
	applyingIndex                int64
	error                        entity.RaftError
	shutdownLatch                *sync.WaitGroup
	lastAppliedLogIndexListeners []LastAppliedLogIndexListener
	applyTaskPool                sync.Pool
	handler                      *applyTaskHandler
	rwMutex                      sync.RWMutex
	sliceRwMutex                 sync.RWMutex
}

func (fci *FSMCallerImpl) Init(ctx context.Context, opt FSMCallerOptions) bool {
	fci.logManager = opt.LogManager
	fci.fsm = opt.FSM
	fci.closureQueue = opt.ClosureQueue
	fci.afterShutdown = opt.AfterShutdown
	fci.node = opt.Node
	fci.error = entity.RaftError{
		ErrType: raft.ErrorType_ErrorTypeNone,
	}
	fci.openHandler(ctx)

	atomic.StoreInt64(&fci.lastAppliedIndex, opt.BootstrapID.GetIndex())
	atomic.StoreInt64(&fci.lastAppliedTerm, opt.BootstrapID.GetTerm())
	utils.RaftLog.Info("Starts FSMCaller successfully.")
	return true
}

func (fci *FSMCallerImpl) openHandler(ctx context.Context) {
	fci.applyTaskPool = sync.Pool{New: func() interface{} {
		return &ApplyTask{}
	}}
	fci.handler = &applyTaskHandler{
		maxCommittedIndex: 0,
		fsmImpl:           fci,
	}

	subCtx, _ := context.WithCancel(ctx)
	utils.CheckErr(utils.RegisterPublisherDefault(subCtx, &ApplyTask{}))
	utils.CheckErr(utils.RegisterSubscriber(fci.handler))
}

func (fci *FSMCallerImpl) Shutdown() {
	var unsafe1 = unsafe.Pointer(fci.shutdownLatch)
	if !atomic.CompareAndSwapPointer(&unsafe1, nil, unsafe.Pointer(&sync.WaitGroup{})) {
		return
	}
	utils.RaftLog.Info("Shutting down FSMCaller...")
	fci.shutdownLatch.Add(1)
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskShutdown
	at.Latch = fci.shutdownLatch
	utils.CheckErr(utils.PublishEvent(at))
	fci.node = nil
	if fci.fsm != nil {
		fci.fsm.OnShutdown()
	}
}

func (fci *FSMCallerImpl) AddLastAppliedLogIndexListener(listener LastAppliedLogIndexListener) {
	defer fci.sliceRwMutex.Unlock()
	fci.sliceRwMutex.Lock()
	fci.lastAppliedLogIndexListeners = append(fci.lastAppliedLogIndexListeners, listener)
}

func (fci *FSMCallerImpl) enqueueTask(at *ApplyTask) bool {
	if fci.shutdownLatch != nil {
		// TODO warn log
		return false
	}
	isOk, err := utils.PublishEventNonBlock(at)
	if err != nil {
		fci.setError(entity.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EINTR, err.Error()),
		})
		return false
	}
	if !isOk {
		fci.setError(entity.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EBUSY, "FSMCaller is overload."),
		})
		return false
	}
	return true
}

func (fci *FSMCallerImpl) setError(err entity.RaftError) {
	if fci.error.ErrType != raft.ErrorType_ErrorTypeNone {
		return
	}
	fci.error = err
	if fci.fsm != nil {
		fci.fsm.OnError(err)
	}
	if fci.node != nil {
		fci.node.OnError(err)
	}
}

func (fci *FSMCallerImpl) OnCommitted(committedIndex int64) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskCommitted
	at.CommittedIndex = committedIndex
	return fci.enqueueTask(at)
}

// just for test
func (fci *FSMCallerImpl) TestFlush() {
	latch := &sync.WaitGroup{}
	latch.Add(1)
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskFlush
	at.Latch = latch
	fci.enqueueTask(at)
	latch.Wait()
}

func (fci *FSMCallerImpl) OnSnapshotLoad(done LoadSnapshotClosure) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskSnapshotLoad
	at.Done = done
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnSnapshotSave(done SaveSnapshotClosure) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskSnapshotSave
	at.Done = done
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnLeaderStop(status entity.Status) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskLeaderStop
	at.Status = &status
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnLeaderStart(term int64) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskLeaderStart
	at.Term = term
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnStartFollowing(context entity.LeaderChangeContext) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskStartFollowing
	ctx := context.Copy()
	at.LeaderChangeContext = &ctx
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnStopFollowing(context entity.LeaderChangeContext) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskStopFollowing
	ctx := context.Copy()
	at.LeaderChangeContext = &ctx
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnError(err entity.RaftError) bool {
	if !fci.error.Status.IsOK() {
		utils.RaftLog.Warn("FSMCaller already in error status, ignore new error: %s", err.Error())
		return false
	}
	closure := &OnErrorClosure{
		Err: err,
	}

	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskError
	at.Done = closure
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) GetLastAppliedIndex() int64 {
	return atomic.LoadInt64(&fci.lastAppliedIndex)
}

func (fci *FSMCallerImpl) Join() {
	if fci.shutdownLatch != nil {
		fci.shutdownLatch.Wait()
		if fci.afterShutdown != nil {
			fci.afterShutdown.Run(entity.StatusOK())
			fci.afterShutdown = nil
		}
		fci.shutdownLatch = nil
	}
}

func (fci *FSMCallerImpl) runApplyTask(task *ApplyTask, maxCommittedIndex int64, endOfBatch bool) int64 {
	var latch *sync.WaitGroup
	defer func() {
		if err := recover(); err != nil {
			utils.RaftLog.Error("runApplyTask occur panic error : %s", err)
		}

		if latch != nil {
			latch.Done()
		}
	}()

	if task.TType == TaskCommitted {
		if task.CommittedIndex > maxCommittedIndex {
			maxCommittedIndex = task.CommittedIndex
		}
	} else {
		if maxCommittedIndex > 0 {
			fci.currTask = TaskCommitted
			fci.doCommitted(maxCommittedIndex)
			maxCommittedIndex = -1
		}
		switch task.TType {
		case TaskCommitted:
			if err := utils.RequireTrue(false, "Impossible"); err != nil {
				panic(err)
			}
		case TaskSnapshotLoad:
			fci.currTask = TaskSnapshotLoad
			if fci.passByStatus(task.Done) {
				fci.doSnapshotLoad(task.Done.(LoadSnapshotClosure))
			}
		case TaskSnapshotSave:
			fci.currTask = TaskSnapshotSave
			if fci.passByStatus(task.Done) {
				fci.doSnapshotSave(task.Done.(SaveSnapshotClosure))
			}
		case TaskLeaderStart:
			fci.currTask = TaskLeaderStart
			fci.doLeaderStart(task.Term)
		case TaskLeaderStop:
			fci.currTask = TaskLeaderStop
			fci.doLeaderStop(*task.Status)
		case TaskStopFollowing:
			fci.currTask = TaskStopFollowing
			fci.doStopFollowing(*task.LeaderChangeContext)
		case TaskStartFollowing:
			fci.currTask = TaskStartFollowing
			fci.doStartFollowing(*task.LeaderChangeContext)
		case TaskIdle:
			utils.RequireFalse(true, "can be")
		case TaskError:
			fci.currTask = TaskError
			fci.doOnError(task.Done.(*OnErrorClosure))
		case TaskShutdown:
			latch = task.Latch
		case TaskFlush:
			latch = task.Latch
		}
	}

	if endOfBatch && maxCommittedIndex >= 0 {
		fci.currTask = TaskCommitted
		fci.doCommitted(maxCommittedIndex)
		maxCommittedIndex = -1
	}
	fci.currTask = TaskIdle
	return maxCommittedIndex
}

func (fci *FSMCallerImpl) doCommitted(committedIndex int64) {
	if !fci.error.Status.IsOK() {
		return
	}
	lastAppliedIndex := atomic.LoadInt64(&fci.lastAppliedIndex)
	if lastAppliedIndex > committedIndex {
		return
	}

	closures := make([]Closure, 0)
	taskClosures := make([]TaskClosure, 0)
	firstClosureIndex := fci.closureQueue.PopClosureUntil(committedIndex, closures, taskClosures)
	fci.onTaskCommitted(taskClosures)

	if err := utils.RequireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex"); err != nil {
		fci.error = entity.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EStateMachine, err.Error()),
		}
		fci.OnError(fci.error)
		return
	}
	iterImpl := &IteratorImpl{
		fsm:               fci.fsm,
		logManager:        fci.logManager,
		closures:          closures,
		firstClosureIndex: firstClosureIndex,
		currentIndex:      lastAppliedIndex,
		committedIndex:    committedIndex,
		applyingIndex:     fci.applyingIndex,
	}

	for iterImpl.IsGood() {
		logEntry := iterImpl.Entry()
		lType := logEntry.LogType
		if lType == raft.EntryType_EntryTypeData {
			fci.doApplyTask(iterImpl)
		} else {
			if lType == raft.EntryType_EntryTypeConfiguration {
				if logEntry.OldPeers != nil && len(logEntry.OldPeers) != 0 {
					fci.fsm.OnConfigurationCommitted(entity.NewConfiguration(logEntry.Peers, entity.EmptyPeers))
				}
			}
			if iterImpl.Done() != nil {
				iterImpl.Done().Run(entity.StatusOK())
			}
			iterImpl.Next()
			continue
		}
	}
	if iterImpl.HasError() {
		fci.setError(*iterImpl.GetError())
		iterImpl.RunTenRestClosureWithError()
	}

	lastIndex := iterImpl.GetIndex() - 1
	lastTerm := fci.logManager.GetTerm(lastIndex)
	atomic.StoreInt64(&fci.lastAppliedIndex, lastIndex)
	fci.lastAppliedTerm = lastTerm
	fci.logManager.SetAppliedID(entity.NewLogID(lastIndex, lastTerm))
	fci.notifyLastAppliedIndexUpdated(lastIndex)
}

func (fci *FSMCallerImpl) doApplyTask(impl *IteratorImpl) {
	iw := NewIteratorWrapper(impl)
	fci.fsm.OnApply(iw)
	if iw.HasNext() {
		utils.RaftLog.Error("Iterator is still valid, did you return before iterator reached the end?")
	}
	iw.Next()
}

func (fci *FSMCallerImpl) onTaskCommitted(closures []TaskClosure) {
	for _, closure := range closures {
		closure.OnCommitted()
	}
}

func (fci *FSMCallerImpl) doSnapshotSave(closure SaveSnapshotClosure) {
	if _, err := utils.RequireNonNil(closure, "SaveSnapshotClosure is nil"); err != nil {
		panic(err)
	}
	lastAppliedIndex := atomic.LoadInt64(&fci.lastAppliedIndex)
	snapshotMeta := raft.SnapshotMeta{}
	snapshotMeta.LastIncludedIndex = lastAppliedIndex
	snapshotMeta.LastIncludedTerm = fci.lastAppliedTerm

	confEntry := fci.logManager.GetConfiguration(lastAppliedIndex)
	if confEntry == nil || confEntry.IsEmpty() {
		utils.RaftLog.Error("Empty conf entry for lastAppliedIndex=%d", lastAppliedIndex)
		st := entity.NewEmptyStatus()
		st.SetError(entity.EINVAL, "Empty conf entry for lastAppliedIndex=%d", lastAppliedIndex)
		closure.Run(st)
		return
	}
	confEntry.GetConf().GetPeers().Range(func(value interface{}) {
		snapshotMeta.Peers = append(snapshotMeta.Peers, value.(*entity.PeerId).GetDesc())
	})
	confEntry.GetConf().GetLearners().Range(func(value interface{}) {
		snapshotMeta.Peers = append(snapshotMeta.Peers, value.(*entity.PeerId).GetDesc())
	})
	oldConf := confEntry.GetOldConf()
	if oldConf != nil {
		oldConf.GetPeers().Range(func(value interface{}) {
			snapshotMeta.Peers = append(snapshotMeta.Peers, value.(*entity.PeerId).GetDesc())
		})
		oldConf.GetLearners().Range(func(value interface{}) {
			snapshotMeta.Peers = append(snapshotMeta.Peers, value.(*entity.PeerId).GetDesc())
		})
	}

	writer := closure.Start(&snapshotMeta)
	if writer == nil {
		closure.Run(entity.NewStatus(entity.EINVAL, "snapshot_storage create SnapshotWriter failed"))
		return
	}
	fci.fsm.OnSnapshotSave(writer, closure)
}

//doSnapshotLoad 加载快照
func (fci *FSMCallerImpl) doSnapshotLoad(closure LoadSnapshotClosure) {
	if _, err := utils.RequireNonNil(closure, "LoadSnapshotClosure is nil"); err != nil {
		panic(err)
	}
	reader := closure.Start()
	if reader == nil {
		closure.Run(entity.NewStatus(entity.EINVAL, "open SnapshotReader failed"))
		return
	}
	snapshotMeta := reader.Load()
	if snapshotMeta == nil {
		closure.Run(entity.NewStatus(entity.EINVAL, "SnapshotReader load SnapshotMeta failed"))
		if reader.Status().GetCode() == entity.EIO {
			fci.setError(entity.RaftError{
				ErrType: raft.ErrorType_ErrorTypeSnapshot,
				Status:  entity.NewStatus(entity.EIO, "Fail to load snapshot meta"),
			})
		}
		return
	}

	lastAppliedID := entity.NewLogID(atomic.LoadInt64(&fci.lastAppliedIndex), fci.lastAppliedTerm)
	// 快照中包含的最后日志条目的索引值以及快照中包含的最后日志条目的任期号
	snapshotID := entity.NewLogID(snapshotMeta.LastIncludedIndex, snapshotMeta.LastIncludedTerm)
	// 首先, 快照的最后日志条目的任期必须要大于或者等于本节点的的 Term, 并且 lastAppliedIndex 必须小于等于快照中包含的最后日志条目的索引值
	// case-one
	//		如果是自己起来之后，加载自己的快照，如果说不满足这个条件，就会出现，加载完这个过期的快照之后，接下来的 LogEntry 找不到的情况，状态机永远无法继续执行下去
	// case-two
	//		如果是接收来自 Leader 的, 那么 Leader 的 lastAppliedIndex 是一定会比自己来的大的，因为只有在 Follower 追随 Leader 的日志太慢的情况下
	//		Leader 才会主动的触发 Follower 从 Leader 同步快照文件的操作
	if lastAppliedID.Compare(snapshotID) > 0 {
		st := entity.NewEmptyStatus()
		st.SetError(entity.ESTALE, "Loading a stale snapshot last_applied_index=%d last_applied_term=%d snapshot_index=%d snapshot_term=%d",
			lastAppliedID.GetIndex(), lastAppliedID.GetTerm(), snapshotID.GetIndex(), snapshotID.GetTerm())
		closure.Run(st)
		return
	}

	// 这里对于用户状态机的快照加载，必须做到阻塞当前状态机的运行
	if !fci.fsm.OnSnapshotLoad(reader) {
		closure.Run(entity.NewStatus(-1, "StateMachine onSnapshotLoad failed"))
		fci.setError(entity.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EStateMachine, "StateMachine onSnapshotLoad failed"),
		})
		return
	}

	if oldPeersCnt := len(snapshotMeta.GetOldPeers()); oldPeersCnt == 0 {
		conf := &entity.Configuration{}
		peers := make([]entity.PeerId, oldPeersCnt)
		for i := 0; i < oldPeersCnt; i++ {
			peer := entity.PeerId{}
			if err := utils.RequireTrue(peer.Parse(snapshotMeta.Peers[i]), "Parse peer failed"); err != nil {
				utils.RaftLog.Error("peer parse from snapshot meta failed : %s", err)
				fci.setError(entity.RaftError{
					ErrType: raft.ErrorType_ErrorTypeStateMachine,
					Status:  entity.NewStatus(entity.EStateMachine, "StateMachine onSnapshotLoad failed, "+err.Error()),
				})
			}
			peers[i] = peer
		}
		conf.AddPeers(peers)
		fci.fsm.OnConfigurationCommitted(conf)
	}
	atomic.StoreInt64(&fci.lastAppliedIndex, snapshotMeta.LastIncludedIndex)
	fci.lastAppliedTerm = snapshotMeta.LastIncludedTerm
	closure.Run(entity.StatusOK())
}

func (fci *FSMCallerImpl) doLeaderStop(status entity.Status) {
	fci.fsm.OnLeaderStop(status)
}

func (fci *FSMCallerImpl) doLeaderStart(term int64) {
	fci.fsm.OnLeaderStart(term)
}

func (fci *FSMCallerImpl) doStartFollowing(ctx entity.LeaderChangeContext) {
	fci.fsm.OnStartFollowing(ctx)
}

func (fci *FSMCallerImpl) doStopFollowing(ctx entity.LeaderChangeContext) {
	fci.fsm.OnStopFollowing(ctx)
}

func (fci *FSMCallerImpl) doOnError(closure *OnErrorClosure) {
	fci.setError(closure.Err)
}

func (fci *FSMCallerImpl) notifyLastAppliedIndexUpdated(lastAppliedIndex int64) {
	defer fci.sliceRwMutex.RUnlock()
	fci.sliceRwMutex.RLock()
	for _, listener := range fci.lastAppliedLogIndexListeners {
		listener.OnApplied(lastAppliedIndex)
	}
}

func (fci *FSMCallerImpl) passByStatus(done Closure) bool {
	st := fci.error.Status
	if !st.IsOK() && done != nil {
		_st := entity.NewEmptyStatus()
		_st.SetError(entity.EINVAL, "FSMCaller is in bad status=%+v", st)
		done.Run(_st)
		return false
	}
	return true
}

// FSMCallerImpl end
