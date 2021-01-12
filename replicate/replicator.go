package replicate

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/pole-group/lraft"
	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/rpc"
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

type ReplicatorEvent int

const (
	Created ReplicatorEvent = iota
	Error
	Destroyed
)

type RequestType int

const (
	SnapshotRequest RequestType = iota
	AppendEntriesRequest
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

type Replicator struct {
	lock                   sync.Cond
	rpcClient              *rpc.RaftRPCClient
	nextIndex              int64
	hasSucceeded           bool
	consecutiveErrorTimes  int64
	timeoutNowIndex        int64
	lastRPCSendTimestamp   int64
	heartbeatCounter       int64
	appendEntriesCounter   int64
	installSnapshotCounter int64
	statInfo               entity.State
	seqGenerator           int64
	rpcInFly               *InFlight
	inFlights              list.List
	options                github.com/pole-group/lraft.ReplicatorOptions
	raftOptions            github.com/pole-group/lraft.RaftOptions
	ctx                    context.Context
}

type InFlight struct {
	reqCnt     int32
	startIndex int64
	size       int32
	future     mono.Mono
	reqType    RequestType
	seq        int64
}

func (isr *InFlight) IsSendingLogEntries() bool {
	return isr.reqType == AppendEntriesRequest && isr.reqCnt > 0
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

func (r *Replicator) Start() {

}

func (r *Replicator) Stop() {

}

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

func (r *Replicator) GetNextSendIndex() int64 {
	if r.inFlights.Len() == 0 {
		return r.nextIndex
	}
	if int64(r.inFlights.Len()) > r.raftOptions.GetMaxReplicatorInflightMsgs() {
		return -1
	}
	if r.rpcInFly != nil && r.rpcInFly.IsSendingLogEntries() {
		return r.rpcInFly.startIndex + int64(r.rpcInFly.reqCnt)
	}
	return -1
}

func (r *Replicator) pollInFlight() *InFlight {
	v := r.inFlights.Front()
	r.inFlights.Remove(v)
	return v.Value.(*InFlight)
}

func (r *Replicator) startHeartbeat(startMs int64) {
	dueTime := startMs + int64(r.options.GetDynamicHeartBeatTimeoutMs())
	utils.DoTimerScheduleByOne(r.ctx, func() {

	}, time.Duration(dueTime)*time.Millisecond)
}

func (r *Replicator) OnVoteReqReturn(resp *core.RequestVoteResponse) {
}

func (r *Replicator) OnHeartbeatReqReturn(resp *core.AppendEntriesResponse) {
}

func (r *Replicator) OnInstallSnapshotReqReturn(resp *core.InstallSnapshotResponse) {

}
