// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	polerpc "github.com/pole-group/pole-rpc"
	"google.golang.org/protobuf/proto"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"
)

const (
	raftSnapshotMetaFile    = "__raft_snapshot_meta"
	raftSnapshotPrefix      = "snapshot_"
	RemoteSnapshotURISchema = "remote://"
	SnapshotTmpPath         = "temp"
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

type LocalSnapshotMetaTable struct {
	metaMap map[string]*raft.LocalFileMeta
	raftOpt RaftOption
	meta    *raft.SnapshotMeta
}

//NewLocalSnapshotMetaTable
func NewLocalSnapshotMetaTable(raftOpt RaftOption) *LocalSnapshotMetaTable {
	return &LocalSnapshotMetaTable{
		metaMap: nil,
		raftOpt: RaftOption{},
		meta:    &raft.SnapshotMeta{},
	}
}

//exportToBytes
func (metaT *LocalSnapshotMetaTable) exportToBytes() ([]byte, error) {
	meaPb := new(raft.LocalSnapshotPbMeta)
	if metaT.hasMeta() {
		meaPb.Meta = metaT.meta
	}
	for k, v := range metaT.metaMap {
		meaPb.Files = append(meaPb.Files, &raft.LocalSnapshotPbMeta_File{
			Name: k,
			Meta: v,
		})
	}
	return proto.Marshal(meaPb)
}

//parseFromBytes
func (metaT *LocalSnapshotMetaTable) parseFromBytes(b []byte) bool {
	if len(b) == 0 {
		utils.RaftLog.Error("null buf to read")
		return false
	}

	pMeta := new(raft.LocalSnapshotPbMeta)
	if err := proto.Unmarshal(b, pMeta); err != nil {
		utils.RaftLog.Error("fail to load meta from buffer")
		return false
	}
	return metaT.loadFromPbMeta(pMeta)
}

//saveToFile
func (metaT *LocalSnapshotMetaTable) saveToFile(path string) bool {
	content, err := metaT.exportToBytes()
	if err != nil {
		utils.RaftLog.Error("LocalSnapshotPbMeta parse to bytes failed, error : %s", err)
		return false
	}
	if err := os.RemoveAll(path); err != nil {
		utils.RaftLog.Error("clear old file or dir failed, error : %s", err)
		return false
	}
	if err := os.WriteFile(path, content, os.ModePerm); err != nil {
		utils.RaftLog.Error("save LocalSnapshotPbMeta : %#v to file : %s failed, error : %s", content, path, err)
		return false
	}
	return true
}

//loadFromPbMeta 从 raft.LocalSnapshotPbMeta 加载数据
func (metaT *LocalSnapshotMetaTable) loadFromPbMeta(pbMeta *raft.LocalSnapshotPbMeta) bool {
	if pbMeta.Meta != nil {
		metaT.meta = pbMeta.Meta
	} else {
		metaT.meta = nil
	}

	metaT.metaMap = make(map[string]*raft.LocalFileMeta)
	for _, f := range pbMeta.Files {
		metaT.metaMap[f.Name] = f.Meta
	}

	return true
}

//loadFromFile 从文件中获取 LocalSnapshotPbMeta
func (metaT *LocalSnapshotMetaTable) loadFromFile(path string) (bool, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return false, err
	}
	pMeta := new(raft.LocalSnapshotPbMeta)
	if err := proto.Unmarshal(b, pMeta); err != nil {
		utils.RaftLog.Error("fail to load meta from buffer")
		return false, err
	}
	return metaT.loadFromPbMeta(pMeta), nil
}

func (metaT *LocalSnapshotMetaTable) hasMeta() bool {
	return metaT.meta != nil && metaT.meta.GetLastIncludedIndex() != 0 && metaT.meta.GetLastIncludedTerm() != 0
}

type LocalSnapshotReader struct {
	readerId  int64
	addr      entity.Endpoint
	metaTable *LocalSnapshotMetaTable
}

func (lsr *LocalSnapshotReader) load() *raft.SnapshotMeta {
	return lsr.metaTable.meta
}

func (lsr *LocalSnapshotReader) generateURIForCopy() string {
	if lsr.addr.IsEmptyEndpoint() || lsr.addr.Equal(entity.AnyEndpoint) {
		utils.RaftLog.Error("address is not specified")
		return ""
	}
	if lsr.readerId == 0 {
		reader := 
	}
	return fmt.Sprintf(RemoteSnapshotURISchema + "%s/%d", lsr.addr.GetDesc(), lsr.readerId)
}

type LocalSnapshotStorage struct {
	refMap                 *polerpc.ConcurrentMap
	path                   string
	addr                   entity.Endpoint
	filterBeforeCopyRemote bool
	lastSnapshotIndex      int64
	lock                   sync.Locker
	raftOpt                RaftOption
	snapshotThrottle       SnapshotThrottle
}

type SnapshotStorageOption struct {
	Uri     string
	RaftOpt RaftOption
}

type SnapshotStorageOptions func(opt *SnapshotStorageOption)

func newSnapshotStorage(options ...SnapshotStorageOptions) (*LocalSnapshotStorage, error) {
	opt := new(SnapshotStorageOption)

	for _, option := range options {
		option(opt)
	}

	return &LocalSnapshotStorage{}, nil
}

func (storage *LocalSnapshotStorage) Init(arg interface{}) error {
	if err := os.MkdirAll(storage.path, os.ModePerm); err != nil && !os.IsExist(err) {
		utils.RaftLog.Error("fail to create dir : %s", storage.path)
		return err
	}
	if !storage.filterBeforeCopyRemote {
		tmpSnapshotPath := filepath.Join(storage.path, SnapshotTmpPath)
		if err := os.RemoveAll(tmpSnapshotPath); err != nil {
			utils.RaftLog.Error("fail to delete temp snapshot path : %s", tmpSnapshotPath)
			return err
		}
	}

	dir, err := ioutil.ReadDir(storage.path)
	if err != nil {
		return err
	}
	snapshots := make([]int, len(dir))
	for i, f := range dir {
		name := f.Name()
		if !strings.HasPrefix(name, raftSnapshotPrefix) {
			continue
		}
		index := utils.ParseToInt(name[len(raftSnapshotPrefix):])
		snapshots[i] = index
	}

	if len(snapshots) != 0 {
		sort.Ints(snapshots)

		for i := 0; i < len(snapshots)-1; i++ {
			index := snapshots[i]
			if !storage.destroySnapshot(int64(index)) {
				return fmt.Errorf("fail to destroy snapshot : %s", filepath.Join(storage.path, fmt.Sprintf("%s%d", raftSnapshotPrefix, index)))
			}
		}

		storage.lastSnapshotIndex = int64(snapshots[len(snapshots)-1])
		storage.refMap.ComputeIfAbsent(storage.lastSnapshotIndex, func(key interface{}) interface{} {
			return utils.NewAtomicInt64()
		})

	}

	return nil
}

//ref 一个简单的计数器饮用计算，必须保证当前的snapshot没有任何引用了才可以结束它的生命周期
func (storage *LocalSnapshotStorage) ref(index int64) {
	atomicInt64 := storage.getRef(index)
	atomicInt64.Increment()
}

func (storage *LocalSnapshotStorage) unref(index int64) {
	ref := storage.getRef(index)
	if ref.Value() == 0 {
		storage.refMap.Remove(index)
		storage.destroySnapshot(index)
	}
}

func (storage *LocalSnapshotStorage) destroySnapshot(index int64) bool {
	path := filepath.Join(storage.path, fmt.Sprintf("%s%d", raftSnapshotPrefix, index))
	if err := os.RemoveAll(path); err != nil {
		utils.RaftLog.Error("fail to destroy snapshot : %s", path)
		return false
	}
	return true
}

func (storage *LocalSnapshotStorage) getRef(index int64) utils.AtomicInt64 {
	storage.refMap.ComputeIfAbsent(index, func(key interface{}) interface{} {
		return utils.NewAtomicInt64()
	})
	return storage.refMap.Get(index).(utils.AtomicInt64)
}

func (storage *LocalSnapshotStorage) SetFilterBeforeCopyRemote() bool {

}

func (storage *LocalSnapshotStorage) Create() SnapshotWriter {

}

//Open 打开一最近的一个快照文件
func (storage *LocalSnapshotStorage) Open() SnapshotReader {
	lsIndex := int64(0)
	storage.lock.Lock()
	if storage.lastSnapshotIndex != 0 {
		lsIndex = storage.lastSnapshotIndex
		storage.ref(lsIndex)
	}
	storage.lock.Unlock()

	if lsIndex == 0 {
		utils.RaftLog.Error("no data for snapshot reader")
		return nil
	}

	snapshotPath := filepath.Join(storage.path, fmt.Sprintf("%s%d", raftSnapshotPrefix, lsIndex))

}

func (storage *LocalSnapshotStorage) CopyFrom(uri string, opts SnapshotCopierOptions) {

}

func (storage *LocalSnapshotStorage) Shutdown() {

}

type GetFileResponseClosure struct {
	RpcResponseClosure
}

type copyFileSession struct {
	lock             sync.Mutex
	st               entity.Status
	finishLatch      sync.WaitGroup
	done             *RpcResponseClosure
	rpcService       *RaftClientOperator
	endpoint         entity.Endpoint
	snapshotThrottle SnapshotThrottle
	req              *raft.GetFileRequest
	raftOpt          *RaftOption
	retryTime        int
	destPath         string
	finished         bool
	timer            polerpc.Future
	rpcCall          polerpc.Future
	maxRetry         int
	retryIntervalMs  int64
	timeoutMs        time.Duration
	f                *os.File
}

type CopySessionOptions func(opt *CopySessionOption)

type CopySessionOption struct {
	req              *raft.GetFileRequest
	RpcService       *RaftClientOperator
	SnapshotThrottle SnapshotThrottle
	RaftOpt          *RaftOption
	Endpoint         entity.Endpoint
	f                *os.File
}

func newCopySession(options ...CopySessionOptions) *copyFileSession {
	opt := new(CopySessionOption)
	opt.req = new(raft.GetFileRequest)
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
		timer:            nil,
		rpcCall:          nil,
		maxRetry:         3,
		retryIntervalMs:  1000,
		timeoutMs:        time.Duration(10*1000) * time.Millisecond,
		req:              opt.req,
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

func (session *copyFileSession) wait() {
	session.finishLatch.Wait()
}

func (session *copyFileSession) close() {
	defer func() {
		_ = session.f.Close()
		session.lock.Unlock()
	}()
	session.lock.Lock()

	if err := session.f.Sync(); err != nil {
		utils.RaftLog.Error("sync file to disk failed, error : %s", err.Error())
	}
}

func (session *copyFileSession) onFinished() {
	if !session.finished {
		if !session.st.IsOK() {

		}
		if session.f != nil {
			_ = session.f.Sync()
			_ = session.f.Close()
		}
		session.finished = true
		session.finishLatch.Done()
	}
}

func (session *copyFileSession) sendNextRpc() {
	defer session.lock.Unlock()
	session.lock.Lock()

	session.timer = nil
	offset := session.req.Offset + session.req.Count
	maxCount := session.raftOpt.MaxByteCountPerRpc

	session.req.Offset = offset
	session.req.Count = int64(maxCount)
	session.req.ReadPartly = true

	if session.finished {
		return
	}

	newMaxCount := int64(maxCount)

	if session.snapshotThrottle != nil {
		newMaxCount = session.snapshotThrottle.ThrottledByThroughput(int64(maxCount))
		if newMaxCount == 0 {
			session.req.Count = 0
			session.timer = polerpc.DelaySchedule(func() {
				session.sendNextRpc()
			}, time.Duration(session.retryIntervalMs)*time.Millisecond)
		}
		return
	}

	session.req.Count = newMaxCount
	req := session.req
	utils.RaftLog.Debug("send get file request %#v to peer %s", req, session.endpoint.GetDesc())
	session.rpcCall = polerpc.NewMonoFuture(session.rpcService.GetFile(session.endpoint, req, &RpcResponseClosure{F: func(resp proto.Message, status entity.Status) {
		session.onRpcReturn(status, resp.(*raft.GetFileResponse))
	}}))
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
		if session.f != nil {
			n, err := session.f.Write(resp.Data)
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

type RemoteFileCopierOption struct {
	Uri              string
	SnapshotThrottle SnapshotThrottle
	Opts             SnapshotCopierOptions
}

type RemoteFileCopierOptions func(opt *RemoteFileCopierOption)

type remoteFileCopier struct {
	readId           int64
	rpcService       *RaftClientOperator
	endpoint         entity.Endpoint
	raftOpt          *RaftOption
	snapshotThrottle SnapshotThrottle
}

func newRemoteFileCopier(options ...RemoteFileCopierOptions) (*remoteFileCopier, bool) {
	opt := new(RemoteFileCopierOption)

	for _, option := range options {
		option(opt)
	}

	if !strings.HasPrefix(opt.Uri, RemoteSnapshotURISchema) {
		utils.RaftLog.Error("invalid url : %s", opt.Uri)
		return nil, false
	}
	newUri := opt.Uri[len(RemoteSnapshotURISchema):]
	slasPos := strings.Index(newUri, "/")
	ipAndPort := newUri[:slasPos]
	newUri = newUri[slasPos+1:]

	readId, err := strconv.ParseInt(newUri, 10, 64)
	if err != nil {
		utils.RaftLog.Error("fail to parse readerId, error : %s", err.Error())
		return nil, false
	}

	address := strings.Split(ipAndPort, ":")

	port, err := strconv.ParseInt(address[1], 10, 64)
	if err != nil {
		utils.RaftLog.Error("fail to parse endpoint port, error : %s", err.Error())
		return nil, false
	}

	endpoint := entity.NewEndpoint(address[0], port)

	if ok, err := opt.Opts.raftClientService.raftClient.CheckConnection(endpoint); !ok || err != nil {
		utils.RaftLog.Error("fail to init channel to %s, error : %#v", endpoint.GetDesc(), err)
		return nil, false
	}

	return &remoteFileCopier{
		readId:           readId,
		rpcService:       opt.Opts.raftClientService,
		endpoint:         endpoint,
		raftOpt:          opt.Opts.raftOpt,
		snapshotThrottle: opt.SnapshotThrottle,
	}, true
}

func (copier *remoteFileCopier) copyToFile(source, destPath string) (bool, error) {
	session, err := copier.startCopyToFile(source, destPath)
	if err != nil {
		return false, err
	}
	defer session.close()
	session.wait()
	return true, nil
}

func (copier *remoteFileCopier) startCopyToFile(source, destPath string) (*copyFileSession, error) {
	f, err := os.Create(destPath)
	if err != nil {
		if os.IsExist(err) {
			if err := os.Remove(destPath); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	session := newCopySession(func(opt *CopySessionOption) {
		opt.req.Filename = source
		opt.req.ReaderID = copier.readId
		opt.SnapshotThrottle = copier.snapshotThrottle
		opt.Endpoint = copier.endpoint
		opt.RaftOpt = copier.raftOpt
		opt.RpcService = copier.rpcService
		opt.f = f
	})
	session.destPath = destPath
	session.sendNextRpc()
	return session, err
}
