// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"sync/atomic"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
)

type DownloadingSnapshot struct {
	Request     *raft.InstallSnapshotRequest
	RequestDone *RpcRequestClosure
}

func NewDownloadingSnapshot(req *raft.InstallSnapshotRequest, done *RpcRequestClosure) *DownloadingSnapshot {
	return &DownloadingSnapshot{
		Request:     req,
		RequestDone: done,
	}
}

type loadSnapshotDone struct {
	reader SnapshotReader
}

func (ld *loadSnapshotDone) Start() SnapshotReader {
	return ld.reader
}

func (ld *loadSnapshotDone) Run(st entity.Status) {
}

type saveSnapshotDone struct {
	writer SnapshotWriter
	done   Closure
	meta   *raft.SnapshotMeta
}

func (sd *saveSnapshotDone) Run(st entity.Status) {
	sd.continueRun(st)
}

func (sd *saveSnapshotDone) continueRun(st entity.Status) {

	if sd.done != nil {
		sd.done.Run(st)
	}
}

func (sd *saveSnapshotDone) Start(meta *raft.SnapshotMeta) SnapshotWriter {
	sd.meta = meta
	return sd.writer
}

func onSnapshotSaveDone(st entity.Status, meta *raft.SnapshotMeta, writer SnapshotWriter, executor *SnapshotExecutor) {

}

type SnapshotExecutorOptions func(opt *SnapshotExecutorOption)

type SnapshotExecutorOption struct {
	Uri                    string
	FsmCaller              FSMCaller
	node                   *nodeImpl
	logMgn                 LogManager
	initTerm               int64
	Addr                   entity.Endpoint
	FilterBeforeCopyRemote bool
	SnapshotThrottle       SnapshotThrottle
}

func newSnapshotExecutor(options ...SnapshotExecutorOptions) (*SnapshotExecutor, error) {
	return nil, nil
}

type SnapshotExecutor struct {
	node                *nodeImpl
	lastSnapshotTerm    int64
	lastSnapshotIndex   int64
	term                int64
	savingSnapshot      int32
	loadingSnapshot     int32
	stopped             int32
	fsmCaller           FSMCaller
	snapshotStorage     SnapshotStorage
	curCopier           SnapshotCopier
	logMgn              LogManager
	loadingSnapshotMeta raft.SnapshotMeta
	downloadingSnapshot atomic.Value // DownloadingSnapshot
}

func (se *SnapshotExecutor) Init(arg interface{}) error {
	opt := arg.(*SnapshotExecutorOption)
	return nil
}

func (se *SnapshotExecutor) Shutdown() {

}

func (se *SnapshotExecutor) GetNode() *nodeImpl {
	return se.node
}

func (se *SnapshotExecutor) DoSnapshot(done Closure) {

}

func (se *SnapshotExecutor) InstallSnapshot(req *raft.InstallSnapshotRequest, done *RpcRequestClosure) {

}

func (se *SnapshotExecutor) stopDownloadingSnapshot(newTerm int64) {

}

func (se *SnapshotExecutor) IsInstallingSnapshot() bool {
	return se.downloadingSnapshot.Load() != nil
}

func (se *SnapshotExecutor) GetSnapshotStorage() SnapshotStorage {
	return se.snapshotStorage
}
