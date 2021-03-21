// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"runtime"

	"github.com/pole-group/lraft/entity"
)

type RpcOptions struct {
	RpcConnectTimeoutMs        int32
	RpcDefaultTimeout          int32
	RpcInstallSnapshotTimeout  int32
	RpcProcessorThreadPoolSize int32
	EnableRpcChecksum          bool
}

func NewDefaultRpcOptions() RpcOptions {
	return RpcOptions{
		RpcConnectTimeoutMs:        1000,
		RpcDefaultTimeout:          5000,
		RpcInstallSnapshotTimeout:  5 * 60 * 1000,
		RpcProcessorThreadPoolSize: 80,
		EnableRpcChecksum:          false,
	}
}

type NodeOptions struct {
	ElectionTimeoutMs        int64
	ElectionMaxDelayMs       int64
	ElectionPriority         entity.ElectionPriority
	DecayPriorityGap         int32
	LeaderLeaseTimeRatio     int32
	SnapshotIntervalSecs     int32
	SnapshotLogIndexMargin   int32
	CatchupMargin            int32
	InitialConf              *entity.Configuration
	Fsm                      StateMachine
	LogURI                   string
	RaftMetaURI              string
	SnapshotURI              string
	FilterBeforeCopyRemote   bool
	DisableCli               bool
	SharedTimerPool          bool
	CliRpcGoroutinePoolSize  int32
	RaftRpcGoroutinePoolSize int32
	EnableMetrics            bool
	SnapshotThrottle         SnapshotThrottle
}

func NewDefaultNodeOptions() NodeOptions {
	return NodeOptions{
		ElectionTimeoutMs:        1000,
		ElectionPriority:         entity.ElectionPriorityDisabled,
		DecayPriorityGap:         10,
		LeaderLeaseTimeRatio:     90,
		SnapshotIntervalSecs:     3600,
		SnapshotLogIndexMargin:   0,
		CatchupMargin:            1000,
		InitialConf:              entity.NewEmptyConfiguration(),
		Fsm:                      nil,
		LogURI:                   "",
		RaftMetaURI:              "",
		SnapshotURI:              "",
		FilterBeforeCopyRemote:   false,
		DisableCli:               false,
		SharedTimerPool:          false,
		CliRpcGoroutinePoolSize:  int32(runtime.NumCPU()),
		RaftRpcGoroutinePoolSize: int32(runtime.NumCPU()) << 2,
		EnableMetrics:            true,
		SnapshotThrottle:         nil,
	}
}

func (opts NodeOptions) getLeaderLeaseTimeoutMs() int64 {
	return opts.ElectionTimeoutMs * int64(opts.LeaderLeaseTimeRatio) / 100
}

type ReadOnlyOption string

const (
	ReadOnlyLeaseBased ReadOnlyOption = "ReadOnlyLeaseBased"
	ReadOnlySafe       ReadOnlyOption = "ReadOnlySafe"
)

type RaftOption struct {
	EnableLogEntryChecksum    bool
	StepDownWhenVoteTimeout   bool
	ReadOnlyOpt               ReadOnlyOption
	MaxReplicatorInflightMsgs int64
	MaxEntriesSize            int32
	MaxAppendBufferEntries    int32
	MaxBodySize               int32
	MaxByteCountPerRpc        int32
}

type replicatorOptions struct {
	dynamicHeartBeatTimeoutMs int32
	electionTimeoutMs         int32
	groupID                   string
	serverId                  entity.PeerId
	peerId                    entity.PeerId
	logMgn                    LogManager
	ballotBox                 *BallotBox
	node                      *nodeImpl
	term                      int64
	snapshotStorage           *LocalSnapshotStorage
	raftRpcOperator           *RaftClientOperator
	replicatorType            ReplicatorType
}

func (r *replicatorOptions) Copy() *replicatorOptions {
	return &replicatorOptions{
		dynamicHeartBeatTimeoutMs: r.dynamicHeartBeatTimeoutMs,
		electionTimeoutMs:         r.electionTimeoutMs,
		groupID:                   r.groupID,
		serverId:                  r.serverId,
		peerId:                    r.peerId,
		logMgn:                    r.logMgn,
		ballotBox:                 r.ballotBox,
		node:                      r.node,
		term:                      r.term,
		snapshotStorage:           r.snapshotStorage,
		raftRpcOperator:           r.raftRpcOperator,
		replicatorType:            r.replicatorType,
	}
}

type BallotBoxOptions struct {
	Waiter       FSMCaller
	ClosureQueue *ClosureQueue
}

type FSMCallerOptions struct {
	LogManager    LogManager
	FSM           StateMachine
	AfterShutdown Closure
	BootstrapID   *entity.LogId
	ClosureQueue  *ClosureQueue
	Node          *nodeImpl
}

type SnapshotCopierOptions struct {
	raftClientService *RaftClientOperator
	raftOpt           *RaftOption
	nodeOpt           *NodeOptions
}
