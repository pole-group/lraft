package github

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	raft "github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/logger"
	"github.com/pole-group/lraft/rafterror"
	"github.com/pole-group/lraft/storage"
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

type LastAppliedLogIndexListener interface {
	OnApplied(lastAppliedLogIndex int64)
}

type ApplyTask struct {
	TType               TaskType
	CommittedIndex      int64
	Term                int64
	Status              *entity.Status
	LeaderChangeContext *entity.LeaderChangeContext
	Done                raft.Closure
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
	return "github.com/pole-group/lraft/ApplyTask"
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

	OnSnapshotLoad(done raft.LoadSnapshotClosure) bool

	OnSnapshotSave(done raft.SaveSnapshotClosure) bool

	OnLeaderStop(status entity.Status) bool

	OnLeaderStart(term int64) bool

	OnStartFollowing(context entity.LeaderChangeContext) bool

	OnStopFollowing(context entity.LeaderChangeContext) bool

	OnError(err rafterror.RaftError) bool

	GetLastAppliedIndex() int64

	Join()
}

type FSMCallerImpl struct {
	logManager                   storage.LogManager
	fsm                          StateMachine
	closureQueue                 *raft.ClosureQueue
	lastAppliedIndex             int64
	lastAppliedTerm              int64
	afterShutdown                raft.Closure
	node                         *NodeImpl
	currTask                     TaskType
	applyingIndex                int64
	error                        rafterror.RaftError
	shutdownLatch                *sync.WaitGroup
	lastAppliedLogIndexListeners []LastAppliedLogIndexListener
	applyTaskPool                sync.Pool
	logger                       logger.Logger
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
	fci.error = rafterror.RaftError{
		ErrType: raft.ErrorType_ErrorTypeNone,
	}
	fci.openHandler(ctx)

	atomic.StoreInt64(&fci.lastAppliedIndex, opt.BootstrapID.GetIndex())
	atomic.StoreInt64(&fci.lastAppliedTerm, opt.BootstrapID.GetTerm())
	fci.logger.Info("Starts FSMCaller successfully.")
	return true
}

func (fci *FSMCallerImpl) SetLogger(logger logger.Logger) {
	fci.logger = logger
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
	fci.logger.Info("Shutting down FSMCaller...")
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
		re := rafterror.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EINTR, err.Error()),
		}
		fci.setError(re)
		return false
	}
	if !isOk {
		re := rafterror.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EBUSY, "FSMCaller is overload."),
		}
		fci.setError(re)
		return false
	}
	return true
}

func (fci *FSMCallerImpl) setError(err rafterror.RaftError) {
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

func (fci *FSMCallerImpl) OnSnapshotLoad(done raft.LoadSnapshotClosure) bool {
	at := fci.applyTaskPool.Get().(*ApplyTask)
	at.Reset()
	at.TType = TaskSnapshotLoad
	at.Done = done
	return fci.enqueueTask(at)
}

func (fci *FSMCallerImpl) OnSnapshotSave(done raft.SaveSnapshotClosure) bool {
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

func (fci *FSMCallerImpl) OnError(err rafterror.RaftError) bool {
	if !fci.error.Status.IsOK() {
		fci.logger.Warn("FSMCaller already in error status, ignore new error: %s", err.Error())
		return false
	}
	closure := &raft.OnErrorClosure{
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
			utils.RequireTrue(false, "Impossible")
		case TaskSnapshotLoad:
			fci.currTask = TaskSnapshotLoad
			if fci.passByStatus(task.Done) {
				fci.doSnapshotLoad(task.Done.(raft.LoadSnapshotClosure))
			}
		case TaskSnapshotSave:
			fci.currTask = TaskSnapshotSave
			if fci.passByStatus(task.Done) {
				fci.doSnapshotSave(task.Done.(raft.SaveSnapshotClosure))
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
			fci.doOnError(task.Done.(*raft.OnErrorClosure))
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

	closures := make([]raft.Closure, 0)
	taskClosures := make([]raft.TaskClosure, 0)
	firstClosureIndex := fci.closureQueue.PopClosureUntil(committedIndex, closures, taskClosures)
	fci.onTaskCommitted(taskClosures)

	utils.RequireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex")
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
		if lType != raft.EntryType_EntryTypeData {
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
		fci.doApplyTask(iterImpl)
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
		fci.logger.Error("Iterator is still valid, did you return before iterator reached the end?")
	}
	iw.Next()
}

func (fci *FSMCallerImpl) onTaskCommitted(closures []raft.TaskClosure) {
	for _, closure := range closures {
		closure.OnCommitted()
	}
}

func (fci *FSMCallerImpl) doSnapshotSave(closure raft.SaveSnapshotClosure) {
	utils.RequireNonNil(closure, "SaveSnapshotClosure is nil")
	lastAppliedIndex := atomic.LoadInt64(&fci.lastAppliedIndex)
	snapshotMeta := raft.SnapshotMeta{}
	snapshotMeta.LastIncludedIndex = lastAppliedIndex
	snapshotMeta.LastIncludedTerm = fci.lastAppliedTerm

	confEntry := fci.logManager.GetConfiguration(lastAppliedIndex)
	if confEntry == nil || confEntry.IsEmpty() {
		fci.logger.Error("Empty conf entry for lastAppliedIndex=%d", lastAppliedIndex)
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

func (fci *FSMCallerImpl) doSnapshotLoad(closure raft.LoadSnapshotClosure) {
	utils.RequireNonNil(closure, "LoadSnapshotClosure is nil")
	reader := closure.Start()
	if reader == nil {
		closure.Run(entity.NewStatus(entity.EINVAL, "open SnapshotReader failed"))
		return
	}
	snapshotMeta := reader.Load()
	if snapshotMeta == nil {
		closure.Run(entity.NewStatus(entity.EINVAL, "SnapshotReader load SnapshotMeta failed"))
		if reader.Status().GetCode() == entity.EIO {
			err := rafterror.RaftError{
				ErrType: raft.ErrorType_ErrorTypeSnapshot,
				Status:  entity.NewStatus(entity.EIO, "Fail to load snapshot meta"),
			}
			fci.setError(err)
		}
		return
	}

	lastAppliedID := entity.NewLogID(atomic.LoadInt64(&fci.lastAppliedIndex), fci.lastAppliedTerm)
	snapshotID := entity.NewLogID(snapshotMeta.LastIncludedIndex, snapshotMeta.LastIncludedTerm)
	if lastAppliedID.Compare(snapshotID) > 0 {
		st := entity.NewEmptyStatus()
		st.SetError(entity.ESTALE, "Loading a stale snapshot last_applied_index=%d last_applied_term=%d snapshot_index=%d snapshot_term=%d", lastAppliedID.GetIndex(), lastAppliedID.GetTerm(), snapshotID.GetIndex(), snapshotID.GetTerm())
		closure.Run(st)
		return
	}

	if !fci.fsm.OnSnapshotLoad(reader) {
		closure.Run(entity.NewStatus(-1, "StateMachine onSnapshotLoad failed"))
		err := rafterror.RaftError{
			ErrType: raft.ErrorType_ErrorTypeStateMachine,
			Status:  entity.NewStatus(entity.EStateMachine, "StateMachine onSnapshotLoad failed"),
		}
		fci.setError(err)
		return
	}

	oldPeersCnt := len(snapshotMeta.GetOldPeers())
	if oldPeersCnt == 0 {
		conf := &entity.Configuration{}
		peers := make([]*entity.PeerId, oldPeersCnt)
		for i := 0; i < oldPeersCnt; i++ {
			peer := &entity.PeerId{}
			utils.RequireTrue(peer.Parse(snapshotMeta.Peers[i]), "Parse peer failed")
			peers[i] = peer
		}
		conf.AddPeers(peers)
	}
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

func (fci *FSMCallerImpl) doOnError(closure *raft.OnErrorClosure) {
	fci.setError(closure.Err)
}

func (fci *FSMCallerImpl) notifyLastAppliedIndexUpdated(lastAppliedIndex int64) {
	defer fci.sliceRwMutex.RUnlock()
	fci.sliceRwMutex.RLock()
	for _, listener := range fci.lastAppliedLogIndexListeners {
		listener.OnApplied(lastAppliedIndex)
	}
}

func (fci *FSMCallerImpl) passByStatus(done raft.Closure) bool {
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

type BallotBox struct {
	waiter             FSMCaller
	closureQueue       *raft.ClosureQueue
	rwMutex            sync.RWMutex
	lastCommittedIndex int64
	pendingIndex       int64
	pendingMetaQueue   *utils.SegmentList
}

func (bx *BallotBox) GetPendingIndex() int64 {
	return bx.pendingIndex
}

func (bx *BallotBox) GetPendingMetaQueue() *utils.SegmentList {
	return bx.pendingMetaQueue
}

func (bx *BallotBox) GetLastCommittedIndex() int64 {
	defer bx.rwMutex.RUnlock()
	bx.rwMutex.RLock()
	return bx.lastCommittedIndex
}

func (bx *BallotBox) Init(opt BallotBoxOptions) {
	utils.RequireFalse(opt.Waiter == nil || opt.ClosureQueue == nil, "waiter or closureQueue is nil.")
	bx.waiter = opt.Waiter
	bx.closureQueue = opt.ClosureQueue
}

func (bx *BallotBox) ClearPendingTasks() {
	defer bx.rwMutex.Unlock()
	bx.rwMutex.Lock()

	bx.pendingMetaQueue.Clear()
	bx.pendingIndex = int64(0)
	bx.closureQueue.Clear()
}

func (bx *BallotBox) RestPendingIndex(newPendingIndex int64) bool {
	defer bx.rwMutex.Unlock()
	bx.rwMutex.Lock()

	isOk := bx.pendingIndex == 0 && bx.closureQueue.IsEmpty()
	if !isOk {
		// TODO error log
		return false
	}
	if newPendingIndex < bx.lastCommittedIndex {
		// TODO error log
		return false
	}
	bx.pendingIndex = newPendingIndex
	bx.closureQueue.RestFirstIndex(newPendingIndex)
	return true
}

func (bx *BallotBox) AppendPendingTask(conf, oldConf *entity.Configuration, done raft.Closure) bool {
	bl := &entity.Ballot{}
	isOk := bl.Init(conf, oldConf)
	if !isOk {
		// TODO error log
		return false
	}

	defer bx.rwMutex.Unlock()
	bx.rwMutex.Lock()

	if bx.pendingIndex <= 0 {
		return false
	}
	bx.pendingMetaQueue.Add(bl)
	bx.closureQueue.AppendPendingClosure(done)
	return true
}

func (bx *BallotBox) SetLastCommittedIndex(lastCommittedIndex int64) bool {
	doUnlock := true
	defer func() {
		if doUnlock {
			bx.rwMutex.Unlock()
		}
	}()

	bx.rwMutex.Lock()
	isOk := bx.pendingIndex != 0 && !bx.closureQueue.IsEmpty()
	if isOk {
		utils.RequireTrue(lastCommittedIndex < bx.pendingIndex, ErrSetLastCommittedIndex, bx.pendingIndex, lastCommittedIndex)
		return false
	}
	if lastCommittedIndex < bx.lastCommittedIndex {
		return false
	}
	if lastCommittedIndex > bx.lastCommittedIndex {
		bx.lastCommittedIndex = lastCommittedIndex
		bx.rwMutex.Unlock()
		doUnlock = false
		bx.waiter.OnCommitted(lastCommittedIndex)
	}
	return true
}

// [firstLogIndex, lastLogIndex] commit to stable at peer
func (bx *BallotBox) CommitAt(firstLogIndex, lastLogIndex int64, peer *entity.PeerId) bool {
	r := bx.innerCommitAt(firstLogIndex, lastLogIndex, peer)
	bx.waiter.OnCommitted(bx.lastCommittedIndex)
	return r
}

func (bx *BallotBox) innerCommitAt(firstLogIndex, lastLogIndex int64, peer *entity.PeerId) bool {
	defer bx.rwMutex.Unlock()
	bx.rwMutex.Lock()
	lastCommittedIndex := int64(0)

	if bx.pendingIndex == 0 {
		return false
	}
	if lastLogIndex < bx.pendingIndex {
		return true
	}
	if lastLogIndex > bx.pendingIndex+int64(bx.pendingMetaQueue.Size()) {
		panic(utils.ErrArrayOutOfBound)
	}

	startAt := int64(math.Max(float64(firstLogIndex), float64(bx.pendingIndex)))
	hint := entity.PosHint{}
	for logIndex := startAt; logIndex <= lastLogIndex; logIndex++ {
		bl := bx.pendingMetaQueue.Get(int32(logIndex - bx.pendingIndex)).(*entity.Ballot)
		hint = bl.GrantWithHint(peer, hint)
		if bl.IsGrant() {
			lastCommittedIndex = logIndex
		}
	}
	if lastCommittedIndex == 0 {
		return true
	}
	bx.pendingMetaQueue.RemoveFromFirst(int32(lastCommittedIndex-bx.pendingIndex) + 1)
	// TODO debug log
	bx.pendingIndex = lastCommittedIndex + 1
	bx.lastCommittedIndex = lastCommittedIndex
	return true
}

func (bx *BallotBox) Shutdown() {
	bx.ClearPendingTasks()
}
