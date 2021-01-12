package github

import (
	"runtime"

	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/storage"
)

type NodeOptions struct {
	ElectionTimeoutMs        int64
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
	SnapshotThrottle         storage.SnapshotThrottle
}

func NewDefaultNodeOptions() NodeOptions {
	return NodeOptions{
		ElectionTimeoutMs:        1000,
		ElectionPriority:         entity.Disabled,
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

type RaftOptions struct {
}

func (ro RaftOptions) GetMaxReplicatorInflightMsgs() int64 {
	return 0
}

type ReplicatorOptions struct {
}

func (ro ReplicatorOptions) GetDynamicHeartBeatTimeoutMs() int32 {
	return -1
}

type BallotBoxOptions struct {
	Waiter       FSMCaller
	ClosureQueue *core.ClosureQueue
}

type FSMCallerOptions struct {
	LogManager    storage.LogManager
	FSM           StateMachine
	AfterShutdown core.Closure
	BootstrapID   *entity.LogId
	ClosureQueue  *core.ClosureQueue
	Node          *NodeImpl
}
