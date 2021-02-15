// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"fmt"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
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
	isInited bool
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

type LogManager interface {
	AddLastLogIndexListener(listener LastLogIndexListener)

	RemoveLogIndexListener(listener LastLogIndexListener)

	Join()

	AppendEntries(entries []*entity.LogEntry, done StableClosure)

	SetSnapshot(meta raft.SnapshotMeta)

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

	SetAppliedID(appliedID *entity.LogId)

	CheckConsistency() entity.Status
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
