// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"sync/atomic"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
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
	opt := new(SnapshotExecutorOption)

	for _, option := range options {
		option(opt)
	}

	executor := &SnapshotExecutor{}
	_, err := executor.Init(opt)
	return executor, err
}

type SnapshotExecutor struct {
	node                *nodeImpl
	lastSnapshotTerm    int64
	lastSnapshotIndex   int64
	term                int64
	savingSnapshot      int32
	loadingSnapshot     bool
	stopped             bool
	fsmCaller           FSMCaller
	snapshotStorage     *LocalSnapshotStorage
	curCopier           SnapshotCopier
	logMgn              LogManager
	runningJobs         int32
	loadingSnapshotMeta raft.SnapshotMeta
	downloadingSnapshot atomic.Value // DownloadingSnapshot
}

func (se *SnapshotExecutor) Init(arg interface{}) (bool, error) {
	opt := arg.(*SnapshotExecutorOption)

	se.logMgn = opt.logMgn
	se.fsmCaller = opt.FsmCaller
	se.node = opt.node
	se.term = opt.initTerm
	storage, err := newSnapshotStorage(func(storageOpt *SnapshotStorageOption) {
		storageOpt.Uri = opt.Uri
		storageOpt.RaftOpt = opt.node.GetRaftOptions()
	})

	if err != nil {
		return false, err
	}

	se.snapshotStorage = storage
	se.snapshotStorage.filterBeforeCopyRemote = opt.FilterBeforeCopyRemote
	se.snapshotStorage.snapshotThrottle = opt.SnapshotThrottle

	if err := se.snapshotStorage.Init(nil); err != nil {
		utils.RaftLog.Error("fail to init snapshot storage")
		return false, err
	}

	se.snapshotStorage.addr = opt.Addr
	reader := storage.Open()
	if reader == nil {
		return true, nil
	}
	meta := reader.Load()
	if meta == nil {
		utils.RaftLog.Error("fail to load meta from : %s", opt.Uri)
		return false, reader.Close()
	}

	utils.RaftLog.Info("loading snapshot, meta : %#v", meta)
	se.loadingSnapshot = true
	se.runningJobs++

	done := newFirstSnapshotLoadDone(reader, func(st entity.Status) {
		se.onSnapshotLoadDone(st)
	})
	if err := utils.RequireTrue(se.fsmCaller.OnSnapshotLoad(done), "first load snapshot must success"); err != nil {
		return false, err
	}

	defer reader.Close()
	done.waitForRun()

	if !done.st.IsOK() {
		utils.RaftLog.Error("fail to load snapshot from %s, FirstSnapshotLoadDone status is %s", opt.Uri, done.st)
		return false, nil
	}

	return true, nil
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

func (se *SnapshotExecutor) registerDownloadingSnapshot(ds *DownloadingSnapshot) {

}

func (se *SnapshotExecutor) onSnapshotLoadDone(st entity.Status) {

}

func (se *SnapshotExecutor) onSnapshotSaveDone(st entity.Status, meta *raft.SnapshotMeta, writer SnapshotWriter) {

}

func (se *SnapshotExecutor) stopDownloadingSnapshot(newTerm int64) {

}

func (se *SnapshotExecutor) IsInstallingSnapshot() bool {
	return se.downloadingSnapshot.Load() != nil
}

func (se *SnapshotExecutor) GetSnapshotStorage() *LocalSnapshotStorage {
	return se.snapshotStorage
}
