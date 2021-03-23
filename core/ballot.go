// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"math"
	"sync"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/utils"
)

type BallotBox struct {
	waiter             FSMCaller
	closureQueue       *ClosureQueue
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
	return bx.closureQueue.RestFirstIndex(newPendingIndex)
}

func (bx *BallotBox) AppendPendingTask(conf, oldConf *entity.Configuration, done Closure) bool {
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

func (bx *BallotBox) SetLastCommittedIndex(lastCommittedIndex int64) (bool, error) {
	doUnlock := true
	defer func() {
		if doUnlock {
			bx.rwMutex.Unlock()
		}
	}()

	bx.rwMutex.Lock()
	isOk := bx.pendingIndex != 0 && !bx.closureQueue.IsEmpty()
	if isOk {
		err := utils.RequireTrue(lastCommittedIndex < bx.pendingIndex, ErrSetLastCommittedIndex, bx.pendingIndex,
			lastCommittedIndex)
		return false, err
	}
	if lastCommittedIndex < bx.lastCommittedIndex {
		return false, nil
	}
	if lastCommittedIndex > bx.lastCommittedIndex {
		bx.lastCommittedIndex = lastCommittedIndex
		bx.rwMutex.Unlock()
		doUnlock = false
		bx.waiter.OnCommitted(lastCommittedIndex)
	}
	return true, nil
}

// [firstLogIndex, lastLogIndex] commit to stable at peer
func (bx *BallotBox) CommitAt(firstLogIndex, lastLogIndex int64, peer entity.PeerId) bool {
	r := bx.innerCommitAt(firstLogIndex, lastLogIndex, peer)
	bx.waiter.OnCommitted(bx.lastCommittedIndex)
	return r
}

func (bx *BallotBox) innerCommitAt(firstLogIndex, lastLogIndex int64, peer entity.PeerId) bool {
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
