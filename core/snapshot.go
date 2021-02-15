// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"io"

	"github.com/golang/protobuf/proto"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
)

const (
	raftSnapshotMetaFile    = "__raft_snapshot_meta"
	raftSnapshotPrefix      = "snapshot_"
	RemoteSnapshotURISchema = "remote://"
)

type SnapshotThrottle interface {
	ThrottledByThroughput(bytes int64) int64
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

