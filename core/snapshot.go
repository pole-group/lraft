// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"io"
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
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

type GetFileResponseClosure struct {
	RpcResponseClosure
}

type copyFileSession struct {
	lock             sync.Mutex
	st               entity.Status
	finishLatch      sync.WaitGroup
	done             *RpcResponseClosure
	rpcService       *rpc.RaftClient
	endpoint         entity.Endpoint
	snapshotThrottle SnapshotThrottle
	req              *raft.GetFileRequest
	raftOpt          RaftOptions
	retryTime        int
	destPath         string
	finished         bool
	ioStream         io.ReadWriteCloser
	timer            polerpc.Future
	rpcCall          polerpc.Future
	maxRetry         int
	retryIntervalMs  int64
	timeoutMs        time.Duration
}

type CopySessionOptions func(opt *CopySessionOption)

type CopySessionOption struct {
	RpcService       *rpc.RaftClient
	SnapshotThrottle SnapshotThrottle
	RaftOpt          RaftOptions
	Endpoint         entity.Endpoint
}

func newCopySession(options ...CopySessionOptions) *copyFileSession {
	opt := new(CopySessionOption)
	for _, f := range options {
		f(opt)
	}

	session := &copyFileSession{
		lock:             sync.Mutex{},
		st:               entity.StatusOK(),
		finishLatch:      sync.WaitGroup{},
		done:             &RpcResponseClosure{},
		rpcService:       opt.RpcService,
		endpoint:         opt.Endpoint,
		snapshotThrottle: opt.SnapshotThrottle,
		raftOpt:          opt.RaftOpt,
		retryTime:        0,
		destPath:         "",
		finished:         false,
		ioStream:         nil,
		timer:            nil,
		rpcCall:          nil,
		maxRetry:         3,
		retryIntervalMs:  1000,
		timeoutMs:        time.Duration(10*1000) * time.Millisecond,
		req:              new(raft.GetFileRequest),
	}

	session.done.F = func(resp proto.Message, status entity.Status) {
		session.onRpcReturn(status, resp.(*raft.GetFileResponse))
	}
	return session
}

func (session *copyFileSession) cancel() {
	defer session.lock.Unlock()
	session.lock.Lock()
	if session.finished {
		return
	}
	if session.timer != nil {
		session.timer.Cancel()
	}
	if session.rpcCall != nil {
		session.rpcCall.Cancel()
	}
	if session.st.IsOK() {
		session.st.SetError(entity.ECanceled, "canceled")
	}
}

func (session *copyFileSession) onFinished() {
	if !session.finished {
		if !session.st.IsOK() {

		}
		if session.ioStream != nil {
			_ = session.ioStream.Close()
		}
		session.finished = true
		session.finishLatch.Done()
	}
}

func (session *copyFileSession) sendNextRpc() {

}

func (session *copyFileSession) onRpcReturn(st entity.Status, resp *raft.GetFileResponse) {
	job := func() bool {
		defer session.lock.Unlock()
		session.lock.Lock()

		if session.finished {
			return true
		}
		if !session.st.IsOK() {
			session.req.Count = 0
			if st.GetCode() == entity.ECanceled {
				if session.st.IsOK() {
					session.st.SetError(st.GetCode(), st.GetMsg())
					session.onFinished()
					return false
				}
			}

			if st.GetCode() != entity.Eagain {
				session.retryTime++
				if session.retryTime+1 >= session.maxRetry {
					if session.st.IsOK() {
						session.st.SetError(st.GetCode(), st.GetMsg())
						session.onFinished()
						return false
					}
				}
			}

			session.timer = polerpc.DelaySchedule(func() {
				polerpc.Go(nil, func(arg interface{}) {
					session.sendNextRpc()
				})
			}, session.timeoutMs)
			return false
		}
		session.retryTime = 0
		if !resp.Eof {
			session.req.Count = resp.ReadSize
		}
		if session.ioStream != nil {
			n, err := session.ioStream.Write(resp.Data)
			if n != len(resp.Data) || err != nil {
				session.st.SetError(entity.EIO, "")
				session.onFinished()
				return false
			}
		} else {

		}
		if resp.Eof {
			session.onFinished()
			return false
		}
		return true
	}
	if job() {
		session.sendNextRpc()
	}
}

type remoteFileCopier struct {
}
