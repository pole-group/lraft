// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pole-group/lraft/entity"
	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/utils"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go/mono"
)

type RunningState int

const (
	Idle RunningState = iota
	Blocking
	AppendingEntries
	InstallingSnapshot
)

type ReplicatorState int32

const (
	ReplicatorProbe ReplicatorState = iota
	ReplicatorSnapshot
	ReplicatorReplicate
	ReplicatorDestroyed
)

type ReplicatorEvent int

const (
	Created ReplicatorEvent = iota
	Error
	Destroyed
)

type RequestType int

const (
	RequestTypeForSnapshot RequestType = iota
	RequestTypeForAppendEntries
)

type ReplicatorStateListener interface {
	OnCreate(peer *entity.PeerId)

	OnError(peer *entity.PeerId, st entity.Status)

	OnDestroyed(peer *entity.PeerId)
}

type Stat struct {
	runningState     RunningState
	firstLogIndex    int64
	lastLogIncluded  int64
	lastLogIndex     int64
	lastTermIncluded int64
}

type InFlight struct {
	reqCnt     int32
	startIndex int64
	size       int32
	future     mono.Mono
	reqType    RequestType
	seq        int64
}

func (ifl *InFlight) IsSendingLogEntries() bool {
	return ifl.reqType == RequestTypeForAppendEntries && ifl.reqCnt > 0
}

type RpcResponse struct {
	status      entity.Status
	req         *proto.Message
	resp        *proto.Message
	rpcSendTime int64
	seq         int64
	reqType     RequestType
}

func (rp *RpcResponse) Compare(other *RpcResponse) int {
	return int(rp.seq - other.seq)
}

//Replicator 这个对象本身，是一个临界资源，log的发送要是竞争的
type Replicator struct {
	lock                   sync.Locker
	raftOperator           *RaftClientOperator
	nextIndex              int64
	hasSucceeded           bool
	consecutiveErrorTimes  int64
	timeoutNowIndex        int64
	lastRPCSendTimestamp   int64
	heartbeatCounter       int64
	appendEntriesCounter   int64
	installSnapshotCounter int64
	statInfo               Stat
	state                  ReplicatorState
	reqSeq                 int64
	requiredNextSeq        int64
	version                int32
	seqGenerator           int64
	rpcInFly               *InFlight
	inFlights              list.List
	options                replicatorOptions
	raftOptions            RaftOptions
	heartbeatInFly         mono.Mono
	timeoutNowInFly        mono.Mono
}

func (r *Replicator) Start() {

}

func (r *Replicator) Stop() {

}

//AddInFlights
func (r *Replicator) AddInFlights(reqType RequestType, startIndex int64, cnt, size int32, seq int64, rpcInfly mono.Mono) {
	r.rpcInFly = &InFlight{
		reqType:    reqType,
		startIndex: startIndex,
		reqCnt:     cnt,
		size:       size,
		seq:        seq,
		future:     rpcInfly,
	}
	r.inFlights.PushBack(r.rpcInFly)
	// TODO metrics
}

//GetNextSendIndex
func (r *Replicator) GetNextSendIndex() int64 {
	if r.inFlights.Len() == 0 {
		return r.nextIndex
	}
	if int64(r.inFlights.Len()) > r.raftOptions.MaxReplicatorInflightMs {
		return -1
	}
	if r.rpcInFly != nil && r.rpcInFly.IsSendingLogEntries() {
		return r.rpcInFly.startIndex + int64(r.rpcInFly.reqCnt)
	}
	return -1
}

//pollInFlight
func (r *Replicator) pollInFlight() *InFlight {
	v := r.inFlights.Front()
	r.inFlights.Remove(v)
	return v.Value.(*InFlight)
}

//pollInFlight
func (r *Replicator) startHeartbeat(startMs int64) {
	dueTime := startMs + int64(r.options.dynamicHeartBeatTimeoutMs)
	time.AfterFunc(time.Duration(dueTime)*time.Millisecond, func() {

	})
}

//installSnapshot 告诉Follower，需要从自己这里拉取snapshot然后在Follower上进行snapshot的load
func (r *Replicator) installSnapshot() {

}

//sendEmptyEntries 发送一个空的LogEntry，用于心跳或者探测
func (r *Replicator) sendEmptyEntries(isHeartbeat bool, heartbeatClosure *AppendEntriesResponseClosure) {
	req := &raft.AppendEntriesRequest{}
	if r.fillCommonFields(req, r.nextIndex-1, isHeartbeat) {
		r.installSnapshot()
		return
	}
	defer func() {
		// 结束当前对复制者的信息发送，需要解放当前的 Replicator
		r.lock.Unlock()
	}()

	sendTime := time.Now()
	if isHeartbeat {
		r.heartbeatCounter++
		if heartbeatClosure == nil {
			heartbeatClosure = &AppendEntriesResponseClosure{RpcResponseClosure{F: func(resp proto.Message, status entity.Status) {
				r.onHeartbeatReqReturn(status, heartbeatClosure.Resp.(*raft.AppendEntriesResponse), sendTime)
			}}}
		}
	} else {
		req.Data = utils.EmptyBytes
		r.statInfo.runningState = AppendingEntries
		r.statInfo.firstLogIndex = r.nextIndex
		r.statInfo.lastLogIncluded = r.nextIndex - 1
		r.appendEntriesCounter++
		atomic.StoreInt32((*int32)(&r.state), int32(ReplicatorProbe))
		stateVersion := r.version
		reqSeq := r.getAndIncrementReqSeq()

		r.raftOperator.AppendEntries(r.options.peerId.GetEndpoint(), req, &AppendEntriesResponseClosure{RpcResponseClosure{
			F: func(resp proto.Message, status entity.Status) {
				r.onRpcReturn(RequestTypeForAppendEntries, status, req, resp, reqSeq, stateVersion, sendTime)
			},
		}}).Subscribe(context.Background())
	}
}

func (r *Replicator) onRpcReturn(reqType RequestType, status entity.Status, req, resp proto.Message,
	seq int64, stateVersion int32, rpcSendTime time.Time) {

}

//SendHeartbeat
func (r *Replicator) SendHeartbeat(closure *AppendEntriesResponseClosure) {
	r.lock.Lock()
}

//onVoteReqReturn
func (r *Replicator) onVoteReqReturn(resp *raft.RequestVoteResponse) {
}

//onHeartbeatReqReturn
func (r *Replicator) onHeartbeatReqReturn(status entity.Status, resp *raft.AppendEntriesResponse, sendTime time.Time) {
}

//onInstallSnapshotReqReturn
func (r *Replicator) onInstallSnapshotReqReturn(resp *raft.InstallSnapshotResponse) {

}

func (r *Replicator) getAndIncrementReqSeq() int64 {
	pre := r.reqSeq
	r.reqSeq++
	if r.reqSeq < 0 {
		r.reqSeq = 0
	}
	return pre
}

func (r *Replicator) fillCommonFields(req *raft.AppendEntriesRequest, prevLogIndex int64, isHeartbeat bool) bool {
	prevLogTerm := r.options.logMgn.GetTerm(prevLogIndex)
	if prevLogTerm == 0 && prevLogIndex != 0 {
		// 如果出现这种情况，则标示，至少在 prevLogIndex 以及 prevLogIndex 之前的 RaftLog 都被 purge 掉了
		if !isHeartbeat {
			if err := utils.RequireTrue(prevLogIndex < r.options.logMgn.GetFirstLogIndex(),
				"prevLogIndex must be less then current log manager first logIndex which logIndex have term"+
					" information"); err != nil {
				// 因为RaftLog被compacted了，因此该LogIndex对应的信息都不在了，无法填充相应的信息数据
				return false
			}
		} else {
			prevLogIndex = 0
		}
	}

	opt := r.options
	req.Term = opt.term
	req.GroupID = opt.groupID
	req.ServerID = opt.serverId.GetDesc()
	req.ServerID = opt.serverId.GetDesc()
	req.PeerID = opt.peerId.GetDesc()
	req.PrevLogIndex = prevLogIndex
	req.PrevLogTerm = prevLogTerm
	req.CommittedIndex = opt.ballotBox.lastCommittedIndex

	return true
}
