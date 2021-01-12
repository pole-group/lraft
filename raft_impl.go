package lraft

import (
	"math"
	"sync/atomic"

	"lraft/entity"
	raft "lraft/proto"
	"lraft/rafterror"
	"lraft/storage"
	"lraft/utils"
)

const (
	ErrRollbackMsg = "StateMachine meet critical error when applying one or more tasks since index=%d, %s"
)

type IteratorImpl struct {
	fsm               StateMachine
	logManager        storage.LogManager
	closures          []Closure
	firstClosureIndex int64
	currentIndex      int64
	committedIndex    int64
	currEntry         *entity.LogEntry
	applyingIndex     int64
	err               *rafterror.RaftError
}

func (iti *IteratorImpl) Entry() *entity.LogEntry {
	return iti.currEntry
}

func (iti *IteratorImpl) GetError() *rafterror.RaftError {
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
		entry := iti.logManager.GetEntry(iti.currentIndex)
		if entry == nil {
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
			utils.RequireNonNil(iti.err, "error")
			done.Run(iti.err.Status)
		}
	}
}

func (iti *IteratorImpl) SetErrorAndRollback(nTail int64, st *entity.Status) {
	utils.RequireTrue(nTail > 0, "Invalid nTail=%d", nTail)
	// TODO 回滚个数需要研究
	if iti.currEntry == nil || iti.currEntry.LogType != raft.EntryType_EntryTypeData {
		iti.currentIndex -= nTail
	} else {
		iti.currentIndex -= nTail - 1
	}
	iti.currEntry = nil
	iti.err = iti.GetOrCreateError()
	iti.err.ErrType = raft.ErrorType_ErrorTypeStateMachine
	iti.err.Status.SetError(entity.EStateMachine, ErrRollbackMsg, iti.currentIndex, utils.IF(st != nil, st, "none"))
}

func (iti *IteratorImpl) GetOrCreateError() *rafterror.RaftError {
	if iti.err == nil {
		iti.err = &rafterror.RaftError{
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

func (iw *IteratorWrapper) SetErrorAndRollback(nTail int64, st entity.Status) {
	iw.impl.SetErrorAndRollback(nTail, &st)
}

func (iw *IteratorWrapper) GetData() []byte {
	entry := iw.impl.Entry()
	return utils.IF(entry != nil, entry.Data, nil).([]byte)
}
