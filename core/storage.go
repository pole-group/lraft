// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"context"
	"fmt"
	"sync"

	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

const (
	ConfPreFix string = "conf_"
)

var (
	FirstLogIdxKey = []byte("meta/firstLogIndex")
)

type RaftMetaStorage struct {
	node     *nodeImpl
	term     int64
	path     string
	voteFor  entity.PeerId
	raftOpts RaftOptions
	isInit   bool
}

type RaftMetaInfo struct {
}

// setTermAndVotedFor
func (rms *RaftMetaStorage) setTermAndVotedFor(term int64, peer entity.PeerId) {

}

func (rms *RaftMetaStorage) reportIOError() {

}

func (rms *RaftMetaStorage) save() {

}

type LogManagerOptions func(opt *LogManagerOption)

type LogManagerOption struct {
}

type LogManager interface {
	AddLastLogIndexListener(listener LastLogIndexListener)

	RemoveLogIndexListener(listener LastLogIndexListener)

	Join()

	AppendEntries(entries []*entity.LogEntry, done Closure) error

	SetSnapshot(meta *raft.SnapshotMeta)

	ClearBufferedLogs()

	GetEntry(index int64) *entity.LogEntry

	GetTerm(index int64) int64

	GetFirstLogIndex() int64

	GetLastLogIndex() int64

	GetLastLogID(isFlush bool) *entity.LogId

	GetConfiguration(index int64) *entity.ConfigurationEntry

	CheckAndSetConfiguration(current *entity.ConfigurationEntry)

	Wait(expectedLastLogIndex int64, cb NewLogCallback, replicator *Replicator) int64

	RemoveWaiter(id int64) bool

	SetAppliedID(appliedID entity.LogId)

	CheckConsistency() entity.Status
}

type LogMgnEventType int32

const (
	LMgnEventForOther LogMgnEventType = iota
	LMgnEventForReset
	LMgnEventForTruncatePrefix
	LMgnEventForTruncateSuffix
	LMgnEventForShutdown
	LMgnEventForLastLogID
)

type StableClosureEvent struct {
	closure *StableClosure
	eType   LogMgnEventType
}

func (sce *StableClosureEvent) Name() string {
	return "StableClosureEvent"
}

func (sce *StableClosureEvent) Sequence() int64 {
	return utils.GetCurrentTimeMs()
}

type waitMeta struct {
	onNewLoh NewLogCallback
	errCode  entity.RaftErrorCode
	arg      *Replicator
}

type logManagerImpl struct {
	opt                   *LogManagerOption
	lock                  sync.RWMutex
	stopped               bool
	hasError              bool
	nextWaitId            int64
	diskId                entity.LogId
	appliedId             entity.LogId
	lastSnapshotId        entity.LogId
	lastID                entity.LogId
	logsInMemory          *polerpc.SegmentList
	waitMap               map[int64]*waitMeta
	fsmCaller             FSMCaller
	confMgn               *entity.ConfigurationManager
	logStorage            LogStorage
	firstLogIndex         int64
	lastLogIndex          int64
	raftOpt               RaftOptions
	shutDownLatch         sync.WaitGroup
	lastLogIndexListeners *polerpc.ConcurrentSlice // < LastLogIndexListener >
	batchOperator         appendBatchOperator
}

func newLogManager(options ...LogManagerOptions) (LogManager, error) {
	opt := new(LogManagerOption)
	for _, option := range options {
		option(opt)
	}

	mgn := &logManagerImpl{}
	return mgn, mgn.init()
}

func (lMgn *logManagerImpl) init() error {
	if err := utils.RegisterSubscriber(lMgn); err != nil {
		return err
	}
	if err := utils.RegisterPublisher(context.Background(), &StableClosureEvent{}, 1024); err != nil {
		return err
	}
	return nil
}

func (lMgn *logManagerImpl) AddLastLogIndexListener(listener LastLogIndexListener) {
	lMgn.lastLogIndexListeners.Add(listener)
}

func (lMgn *logManagerImpl) RemoveLogIndexListener(listener LastLogIndexListener) {
	lMgn.lastLogIndexListeners.Remove(listener)
}

func (lMgn *logManagerImpl) Join() {

}

func (lMgn *logManagerImpl) AppendEntries(entries []*entity.LogEntry, done Closure) error {
	if err := utils.RequireTrue(done != nil, "done must not be nil"); err != nil {
		return err
	}

}

func (lMgn *logManagerImpl) SetSnapshot(meta *raft.SnapshotMeta) {

}

func (lMgn *logManagerImpl) ClearBufferedLogs() {

}

func (lMgn *logManagerImpl) clearMemoryLogs(id entity.LogId) {
	defer lMgn.lock.Unlock()
	lMgn.lock.Lock()

	lMgn.logsInMemory.RemoveFromFirstWhen(func(v interface{}) bool {
		entry := v.(*entity.LogEntry)
		return entry.LogID.Compare(id) <= 0
	})
}

func (lMgn *logManagerImpl) GetEntry(index int64) *entity.LogEntry {
	if index <= 0 {
		return nil
	}

	// 优先从 raft log in memory 中获取
	fromMemory := func() *entity.LogEntry {
		defer lMgn.lock.RUnlock()
		lMgn.lock.RLock()

		if index < lMgn.firstLogIndex || index > lMgn.lastLogIndex {
			return nil
		}
		return lMgn.getEntryFromMemory(index)
	}
	entry := fromMemory()
	if entry != nil {
		return entry
	}
	entry = lMgn.logStorage.GetEntry(index)
	if entry == nil {
		lMgn.reportError(entity.EIO, fmt.Sprintf("Corrupted entry at index=%d, not found", index))
		return entry
	}
	if lMgn.raftOpt.EnableLogEntryChecksum && entry.IsCorrupted() {
		lMgn.reportError(entity.EIO, fmt.Sprintf("Corrupted entry at index=%d, term=%d, expectedChecksum=%d, "+
			"realChecksum=%d", index, entry.LogID.GetTerm(), entry.GetChecksum(), entry.Checksum()))
	}
	return entry
}

//GetTerm 获取某个日志的对应的 term 信息
func (lMgn *logManagerImpl) GetTerm(index int64) int64 {
	if index <= 0 {
		return 0
	}
	fromMemory := func() int64 {
		defer lMgn.lock.RUnlock()
		lMgn.lock.RLock()

		if index == lMgn.lastSnapshotId.GetIndex() {
			return lMgn.lastSnapshotId.GetTerm()
		}
		if index > lMgn.lastLogIndex || index < lMgn.firstLogIndex {
			return 0
		}
		entry := lMgn.getEntryFromMemory(index)
		if entry != nil {
			return entry.LogID.GetTerm()
		}
		return -1
	}

	term := fromMemory()
	if term != -1 {
		return term
	}
	return lMgn.getTermFromLogStorage(index)
}

//getTermFromLogStorage 从持久化存储中的日志信息获取对应日志的任期
func (lMgn *logManagerImpl) getTermFromLogStorage(index int64) int64 {
	entry := lMgn.logStorage.GetEntry(index)
	if entry == nil {
		lMgn.reportError(entity.EIO, fmt.Sprintf("Corrupted entry at index=%d, not found", index))
		return 0
	}
	if lMgn.raftOpt.EnableLogEntryChecksum && entry.IsCorrupted() {
		lMgn.reportError(entity.EIO, fmt.Sprintf("Corrupted entry at index=%d, term=%d, expectedChecksum=%d, "+
			"realChecksum=%d", index, entry.LogID.GetTerm(), entry.GetChecksum(), entry.Checksum()))
	}
	return entry.LogID.GetTerm()
}

func (lMgn *logManagerImpl) GetFirstLogIndex() int64 {
	return lMgn.logStorage.GetFirstLogIndex()
}

func (lMgn *logManagerImpl) GetLastLogIndex() int64 {
	return lMgn.getLastLogIndex(false)
}

//getLastLogIndex 根据 flush 参数来判断是否需要先将所有的 Raft Log 数据持久化之后再获取最新的日志索引信息
func (lMgn *logManagerImpl) getLastLogIndex(flush bool) int64 {
	var done *StableClosure

	f := func() int64 {
		defer lMgn.lock.RUnlock()
		lMgn.lock.RLock()
		if !flush {
			return lMgn.lastLogIndex
		}
		if lMgn.lastLogIndex == lMgn.lastSnapshotId.GetIndex() {
			return lMgn.lastLogIndex
		}

		done = &StableClosure{
			firstLogIndex: 0,
			nEntries:      0,
			latch:         sync.WaitGroup{},
		}
		done.latch.Add(1)
		done.f = func(status entity.Status) {
			done.latch.Done()
		}

		ok, err := utils.PublishEventNonBlock(&StableClosureEvent{
			closure: done,
			eType:   LMgnEventForLastLogID,
		})
		if !ok || err != nil {
			msg := fmt.Sprintf("Log manager is overload, error info : %#v", err)
			lMgn.reportError(entity.Ebusy, msg)
			polerpc.Go(entity.NewStatus(entity.Ebusy, msg), func(arg interface{}) {
				st := arg.(entity.Status)
				done.Run(st)
			})
		}
		return -1
	}
	index := f()
	if index != -1 {
		return index
	}

	done.latch.Wait()
	return done.lastLogID.GetIndex()
}

func (lMgn *logManagerImpl) GetLastLogID(isFlush bool) *entity.LogId {
	defer lMgn.lock.RUnlock()
	lMgn.lock.RLock()

	if !isFlush {

	}
}

func (lMgn *logManagerImpl) GetConfiguration(index int64) *entity.ConfigurationEntry {

}

func (lMgn *logManagerImpl) CheckAndSetConfiguration(current *entity.ConfigurationEntry) {

}

func (lMgn *logManagerImpl) Wait(expectedLastLogIndex int64, cb NewLogCallback, replicator *Replicator) int64 {
	wm := &waitMeta{
		onNewLoh: cb,
		errCode:  0,
		arg:      replicator,
	}
	return lMgn.notifyOnNewLog(expectedLastLogIndex, wm)
}

func (lMgn *logManagerImpl) notifyOnNewLog(expectedLastLogIndex int64, wm *waitMeta) int64 {
	defer lMgn.lock.Unlock()
	lMgn.lock.Lock()

	if expectedLastLogIndex != lMgn.lastLogIndex || lMgn.stopped {
		if lMgn.stopped {
			wm.errCode = entity.EStop
		}
		polerpc.Go(wm, func(arg interface{}) {
			wm := arg.(*waitMeta)
			wm.onNewLoh.OnNewLog(wm.arg, wm.errCode)
		})
		return 0
	}
	waitId := lMgn.nextWaitId
	lMgn.nextWaitId++
	lMgn.waitMap[waitId] = wm
	return waitId
}

//wakeupAllWaiter 唤醒所有的 NewLogCallback
func (lMgn *logManagerImpl) wakeupAllWaiter(lock sync.Locker) bool {
	if len(lMgn.waitMap) == 0 {
		lock.Unlock()
		return false
	}

	waitMetas := lMgn.waitMap
	errCode := utils.IF(lMgn.stopped, entity.EStop, entity.Success).(entity.RaftErrorCode)
	lMgn.waitMap = make(map[int64]*waitMeta)
	lock.Unlock()
	for _, wm := range waitMetas {
		wm.errCode = errCode
		polerpc.Go(wm, func(arg interface{}) {
			wm := arg.(*waitMeta)
			wm.onNewLoh.OnNewLog(wm.arg, wm.errCode)
		})
	}

	return true
}

func (lMgn *logManagerImpl) RemoveWaiter(id int64) bool {
	defer lMgn.lock.Unlock()
	lMgn.lock.Lock()
	_, ok := lMgn.waitMap[id]
	delete(lMgn.waitMap, id)
	return ok
}

func (lMgn *logManagerImpl) SetAppliedID(appliedID entity.LogId) {
	lMgn.lock.Lock()
	if lMgn.appliedId.Compare(appliedID) <= 0 {
		lMgn.appliedId = appliedID
	}
	lMgn.lock.Unlock()
}

//CheckConsistency 一致性的校验，主要是检查当前持久化存储的日志的firstLogIndex、lastLogIndex以及lastSnapshotId之间LogIndex的大小关系
func (lMgn *logManagerImpl) CheckConsistency() entity.Status {
	defer lMgn.lock.RUnlock()
	lMgn.lock.RLock()
	if err := utils.RequireTrue(lMgn.firstLogIndex > 0, "firstLogIndex must be grate then zero"); err != nil {
		return entity.NewStatus(entity.EInternal, err.Error())
	}
	if err := utils.RequireTrue(lMgn.lastLogIndex >= 0, "lastLogIndex must be grate then or equal zero"); err != nil {
		return entity.NewStatus(entity.EInternal, err.Error())
	}
	if lMgn.lastSnapshotId.IsEquals(entity.NewLogID(0, 0)) {
		if lMgn.firstLogIndex == 1 {
			return entity.StatusOK()
		}
		return entity.NewStatus(entity.EIO, fmt.Sprintf("Missing logs in (0, %d)", lMgn.firstLogIndex))
	}
	if lMgn.lastSnapshotId.GetIndex() >= lMgn.firstLogIndex-1 && lMgn.lastSnapshotId.GetIndex() <= lMgn.lastLogIndex {
		return entity.StatusOK()
	}
	return entity.NewStatus(entity.EIO, fmt.Sprintf("There's a gap between snapshot={%d, %d} and log=[%d, %d]",
		lMgn.lastSnapshotId.GetTerm(), lMgn.lastSnapshotId.GetIndex(), lMgn.firstLogIndex, lMgn.lastLogIndex))
}

func (lMgn *logManagerImpl) getEntryFromMemory(index int64) *entity.LogEntry {
	if index < 0 {
		return nil
	}
	if lMgn.logsInMemory.IsEmpty() {
		return nil
	}
	firstLogIndex := lMgn.logsInMemory.PeekFirst().(*entity.LogEntry).LogID.GetIndex()
	lastLogIndex := lMgn.logsInMemory.PeekLast().(*entity.LogEntry).LogID.GetIndex()
	if lastLogIndex-firstLogIndex+1 != int64(lMgn.logsInMemory.Size()) {
		lMgn.reportError(entity.EInternal, "invalid raft log info in memory")
		return nil
	}
	if index >= firstLogIndex && index <= lastLogIndex {
		v, err := lMgn.logsInMemory.Get(int32(index - firstLogIndex))
		if err == nil {
			return v.(*entity.LogEntry)
		}
		utils.RaftLog.Error("get raft log from memory list failed, error : %s", err)
	}
	return nil
}

func (lMgn *logManagerImpl) notifyLastLogIndexListeners() {
	lMgn.lastLogIndexListeners.ForEach(func(index int, v interface{}) {
		listener := v.(LastLogIndexListener)
		listener.OnLastLogIndexChanged(lMgn.lastLogIndex)
	})
}

type appendBatchOperator struct {
	lMgn       *logManagerImpl
	closures   []*StableClosure
	cap        int32
	size       int32
	bufferSize int32
	toAppend   []*entity.LogEntry
	lastID     entity.LogId
}

func (abo appendBatchOperator) append(closure *StableClosure) {
	if abo.size == abo.cap || abo.bufferSize >= abo.lMgn.raftOpt.MaxAppendBufferEntries {
		abo.flush()
	}
	abo.closures = append(abo.closures, closure)
	abo.size++
	abo.toAppend = append(abo.toAppend, closure.entries...)
	for _, entry := range closure.entries {
		abo.bufferSize += int32(len(entry.Data))
	}
}

func (abo appendBatchOperator) flush() entity.LogId {
	if abo.size > 0 {
		abo.lastID = abo.lMgn.appendToStorage(abo.toAppend)
		for _, closure := range abo.closures {
			closure.entries = make([]*entity.LogEntry, 0)
			var st entity.Status
			if abo.lMgn.hasError {
				st = entity.NewStatus(entity.EIO, "Corrupted LogStorage")
			} else {
				st = entity.StatusOK()
			}
			closure.Run(st)
		}
		abo.closures = make([]*StableClosure, 0)
		abo.toAppend = make([]*entity.LogEntry, 0)
	}
	abo.size = 0
	abo.bufferSize = 0
	return abo.lastID
}

func (lMgn *logManagerImpl) setDiskID(id entity.LogId) {
	if entity.IsEmptyLogID(id) {
		return
	}

	var clearID entity.LogId
	lMgn.lock.Lock()

	if lMgn.diskId.Compare(id) > 0 {
		return
	}
	lMgn.diskId = id
	clearID = utils.IF(lMgn.diskId.Compare(lMgn.appliedId) <= 0, lMgn.diskId, lMgn.appliedId).(entity.LogId)
	lMgn.lock.Unlock()

	if !entity.IsEmptyLogID(clearID) {
		lMgn.clearMemoryLogs(clearID)
	}
}

func (lMgn *logManagerImpl) appendToStorage(entries []*entity.LogEntry) entity.LogId {
	lastID := entity.NewEmptyLogID()
	if !lMgn.hasError {
		entriesCount := len(entries)
		nAppend, err := lMgn.logStorage.AppendEntries(entries)
		if err != nil {
			utils.RaftLog.Error("append entries to logStorage failed, error : %s", err)
			lMgn.reportError(entity.EIO, err.Error())
			return lastID
		}
		if nAppend != entriesCount {
			lMgn.reportError(entity.EIO, "Fail to append log entries")
		}
		if nAppend > 0 {
			lastID = entries[nAppend-1].LogID
		}
	}
	return lastID
}

func (lMgn *logManagerImpl) reportError(errCode entity.RaftErrorCode, msg string) {
	lMgn.hasError = true
	err := entity.RaftError{
		ErrType: raft.ErrorType_ErrorTypeLog,
		Status:  entity.NewStatus(errCode, msg),
	}
	lMgn.fsmCaller.OnError(err)
}

func (lMgn *logManagerImpl) OnEvent(event utils.Event, endOfBatch bool) {
	sEvent := event.(*StableClosureEvent)
	if sEvent.eType == LMgnEventForShutdown {
		lMgn.lastID = lMgn.batchOperator.flush()
		lMgn.setDiskID(lMgn.lastID)
		lMgn.shutDownLatch.Done()
		return
	}

	done := sEvent.closure
	if done.entries != nil && len(done.entries) != 0 {
		lMgn.batchOperator.append(done)
	} else {
		lMgn.lastID = lMgn.batchOperator.flush()
		ret := true
		switch sEvent.eType {
		case LMgnEventForLastLogID:
			done.lastLogID = lMgn.lastID
		case LMgnEventForReset:
			var err error
			ret, err = lMgn.logStorage.Rest(done.nextLogIndex)
			if err != nil {
				utils.RaftLog.Error("rest logStorage when do snapshot failed, error : %s", err)
			}
		case LMgnEventForTruncatePrefix:
			utils.RaftLog.Debug("truncating storage to firstIndexKept=%d", done.firstIndexKept)
			ret = lMgn.logStorage.TruncatePrefix(done.firstIndexKept)
		case LMgnEventForTruncateSuffix:
			utils.RaftLog.Debug("truncating storage to lastIndexKept=%d", done.lastIndexKept)
			ret = lMgn.logStorage.TruncateSuffix(done.lastIndexKept)
			if ret {
				lMgn.lastID.SetIndex(done.lastIndexKept)
				lMgn.lastID.SetTerm(done.lastTermKept)
				if err := utils.RequireTrue(lMgn.lastID.GetIndex() == 0 || lMgn.lastID.GetTerm() != 0, ""); err != nil {
					ret = false
				}
			}
		default:
			//do nothing
		}

		if !ret {
			lMgn.reportError(entity.EIO, "Failed operation in LogStorage")
		} else {
			done.Run(entity.StatusOK())
		}
	}

	if endOfBatch {
		lMgn.lastID = lMgn.batchOperator.flush()
		lMgn.setDiskID(lMgn.lastID)
	}
}

func (lMgn *logManagerImpl) IgnoreExpireEvent() bool {
	return false
}

func (lMgn *logManagerImpl) SubscribeType() utils.Event {
	return &StableClosureEvent{}
}

type LogStorage interface {
	GetFirstLogIndex() int64

	GetLastLogIndex() int64

	GetEntry(index int64) *entity.LogEntry

	GetTerm(index int64) int64

	AppendEntry(entry *entity.LogEntry) (bool, error)

	AppendEntries(entries []*entity.LogEntry) (int, error)

	TruncatePrefix(firstIndexKept int64) bool

	TruncateSuffix(lastIndexKept int64) bool

	//Rest always use when install snapshot from leader, will clear all exits log and set nextLogIndex
	Rest(nextLogIndex int64) (bool, error)

	Shutdown() error
}

type LogStorageType int16

const (
	PebbleLogStorageType LogStorageType = iota
	SimpleStorageType
)

func NewLogStorage(storageType LogStorageType, options ...LogStorageOptions) (LogStorage, error) {
	switch storageType {
	case PebbleLogStorageType:
		return newPebbleStorage(options...)
	case SimpleStorageType:
		return newSimpleFileStorage(options...)
	}
	return nil, fmt.Errorf("impossible run here, unsupport LogStorage Type")
}

type LogStorageOption struct {
	KvDir    string //数据目录
	WALDir   string //WAL目录
	WriteOpt struct { //写操作相关的配置
		Sync bool
	}
	ConfMgn     *entity.ConfigurationManager //entity.ConfigurationManager
	ExtendParam map[string]string            //额外的配置信息，和具体的存储实现有关
}

type LogStorageOptions func(opt *LogStorageOption)

type truncateLog struct {
	firstIndex  int64
	secondIndex int64
}
