// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"io"
	"math"
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

const (
	raftSnapshotMetaFile    = "__raft_snapshot_meta"
	raftSnapshotPrefix      = "snapshot_"
	RemoteSnapshotURISchema = "remote://"
)

type SnapshotThrottle interface {
	ThrottledByThroughput(bytes int64) int64
}

type ThroughputSnapshotThrottle struct {
	throttleThroughputBytes   int64
	checkCycleSecs            int64
	lastThroughputCheckTimeUs int64
	currThroughputBytes       int64
	lock                      sync.Mutex
	baseAligningTimeUs        int64
}

func newSnapshotThrottle(throttleThroughputBytes, checkCycleSecs int64) SnapshotThrottle {
	return &ThroughputSnapshotThrottle{
		throttleThroughputBytes:   throttleThroughputBytes,
		checkCycleSecs:            checkCycleSecs,
		lastThroughputCheckTimeUs: calculateCheckTimeUs(utils.GetCurrentTimeMs(), 1000*1000/checkCycleSecs),
		currThroughputBytes:       0,
		lock:                      sync.Mutex{},
		baseAligningTimeUs:        1000 * 1000 / checkCycleSecs,
	}
}

//ThrottledByThroughput 计算下次可以发送多少数据
func (t *ThroughputSnapshotThrottle) ThrottledByThroughput(bytes int64) int64 {
	availableSize := int64(0)
	nowUs := utils.GetCurrentTimeNs()
	limitPerCycle := t.throttleThroughputBytes / t.checkCycleSecs

	defer t.lock.Unlock()
	t.lock.Lock()

	if t.currThroughputBytes+bytes > limitPerCycle {
		if nowUs-t.lastThroughputCheckTimeUs <= 1000*1000/t.checkCycleSecs {
			availableSize = limitPerCycle - t.currThroughputBytes
			t.currThroughputBytes = limitPerCycle
		} else {
			availableSize = int64(math.Min(float64(bytes), float64(limitPerCycle)))
			t.currThroughputBytes = availableSize
			t.lastThroughputCheckTimeUs = calculateCheckTimeUs(nowUs, t.baseAligningTimeUs)
		}
	} else {
		availableSize = bytes
		t.currThroughputBytes += availableSize
	}

	return availableSize
}

func calculateCheckTimeUs(currTimeUs, baseAligningTimeUs int64) int64 {
	return currTimeUs / baseAligningTimeUs * baseAligningTimeUs
}

type Snapshot interface {
	GetPath() string

	ListFiles() []string

	GetFileMeta(fileName string) proto.Message
}

type SnapshotReader interface {
	Snapshot

	Status() entity.Status

	Load() *raft.SnapshotMeta

	GenerateURIForCopy() string

	io.Closer
}

type SnapshotWriter interface {
	SaveMeta(meta raft.SnapshotMeta) bool

	AddFile(fileName string, meta proto.Message)

	RemoveFile(fileName string)

	Close(keepDataOnError bool)
}

type SnapshotCopier interface {
	Cancel()

	Join()

	Start()

	GetReader() SnapshotReader
}

type LastLogIndexListener interface {
	//OnLastLogIndexChanged 当最新的LogIndex发生变化时的监听，这里不能panic error，所有的 error 必须自行 defer recover 处理
	OnLastLogIndexChanged(lastLogIndex int64)
}

type NewLogCallback interface {
	OnNewLog(arg *Replicator, errCode entity.RaftErrorCode)
}

type SnapshotStorage interface {
	SetFilterBeforeCopyRemote() bool

	Create() SnapshotWriter

	Open() SnapshotReader

	CopyFrom(uri string, opts SnapshotCopierOptions)
}

type copyFileSession struct {
}

type remoteFileCopier struct {
}
