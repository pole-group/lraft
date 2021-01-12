package github

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go/mono"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"

	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/logger"
	"github.com/pole-group/lraft/rafterror"
	"github.com/pole-group/lraft/replicate"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/storage"
	"github.com/pole-group/lraft/transport"
	"github.com/pole-group/lraft/utils"
)

type LogEntryAndClosure struct {
	Entry        *entity.LogEntry
	Done         core.Closure
	ExpectedTerm int64
	Latch        *sync.WaitGroup
}

func (lac *LogEntryAndClosure) Reset() {
	lac.Entry = nil
	lac.Done = nil
	lac.Latch = nil
	lac.ExpectedTerm = -1
}

func (lac *LogEntryAndClosure) Name() string {
	return "github.com/pole-group/lraft/LogEntryAndClosure"
}

func (lac *LogEntryAndClosure) Sequence() int64 {
	return utils.GetCurrentTimeMs()
}

type Stage int16

const (
	StageNone Stage = iota
	StageCatchingUp
	StageJoint
	StageStable
)

type ConfigurationCtx struct {
	node        *NodeImpl
	stage       Stage
	nChanges    int32
	version     int64
	newPeers    []*entity.PeerId
	oldPeers    []*entity.PeerId
	addingPeers []*entity.PeerId

	learners    []*entity.PeerId
	oldLearners []*entity.PeerId
	done        core.Closure
}

func NewConfigurationCtx(node *NodeImpl) *ConfigurationCtx {
	return &ConfigurationCtx{
		node:    node,
		stage:   StageNone,
		version: 0,
	}
}

func (cc *ConfigurationCtx) Start(conf, oldConf *entity.Configuration, done core.Closure) {

}

func (cc *ConfigurationCtx) IsBusy() bool {
	return cc.stage != StageNone
}

type Node interface {
	GetLeaderID() *entity.PeerId

	GetNodeID() *entity.NodeId

	GetGroupID() string

	GetOptions() NodeOptions

	GetRaftOptions() RaftOptions

	IsLeader() bool

	Shutdown(done core.Closure)

	Join()

	Apply(task *entity.Task)

	ReadIndex(reqCtx []byte, done *core.ReadIndexClosure)

	ListPeers() []*entity.PeerId

	ListAlivePeers() []*entity.PeerId

	ListLearners() []*entity.PeerId

	ListAliceLearners() []*entity.PeerId

	AddPeer(peer *entity.PeerId, done core.Closure)

	RemovePeer(peer *entity.PeerId, done core.Closure)

	ChangePeers(newConf *entity.Configuration, done core.Closure)

	ResetPeers(newConf *entity.Configuration) entity.Status

	AddLearners(learners []*entity.PeerId, done core.Closure)

	RemoveLearners(learners []*entity.PeerId, done core.Closure)

	ResetLearners(learners []*entity.PeerId, done core.Closure)

	Snapshot(done core.Closure)

	ResetElectionTimeoutMs(electionTimeoutMs int32)

	TransferLeadershipTo(peer *entity.PeerId) entity.Status

	ReadCommittedUserLog(index int64) *entity.UserLog

	AddReplicatorStateListener(replicatorStateListener replicate.ReplicatorStateListener)

	RemoveReplicatorStateListener(replicatorStateListener replicate.ReplicatorStateListener)

	ClearReplicatorStateListeners()

	GetReplicatorStatueListeners() []replicate.ReplicatorStateListener

	GetNodeTargetPriority() int32
}

type NodeState int

const (
	StateLeader NodeState = iota
	StateTransferring
	StateCandidate
	StateFollower
	StateError
	StateUninitialized
	StateShutting
	StateShutdown
	StateEnd
)

func (ns NodeState) GetName() string {
	switch ns {
	case StateLeader:
		return "StateLeader"
	case StateTransferring:
		return "StateTransferring"
	case StateCandidate:
		return "StateCandidate"
	case StateFollower:
		return "StateFollower"
	case StateUninitialized:
		return "StateUninitialized"
	case StateShutting:
		return "StateShutting"
	case StateShutdown:
		return "StateShutdown"
	case StateEnd:
		return "StateEnd"
	default:
		return "UnKnowState"
	}
}

func IsNodeActive(state NodeState) bool {
	return state < StateError
}

type NodeImpl struct {
	rwMutex             sync.RWMutex
	state               NodeState
	nodeID              entity.NodeId
	logger              logger.Logger
	currTerm            int64
	leaderID            *entity.PeerId
	firstLogIndex       int64
	nEntries            int32
	lastLeaderTimestamp int64
	options             NodeOptions
	raftOptions         RaftOptions
	ballotBox           *BallotBox
	serverID            *entity.PeerId
	handler             *raftRpcHandler
	logManager			storage.LogManager
}

func (ni *NodeImpl) GetLeaderID() *entity.PeerId {

}

func (ni *NodeImpl) GetNodeID() *entity.NodeId {

}

func (ni *NodeImpl) GetGroupID() string {

}

func (ni *NodeImpl) GetOptions() NodeOptions {

}

func (ni *NodeImpl) GetRaftOptions() RaftOptions {

}

func (ni *NodeImpl) IsLeader() bool {

}

func (ni *NodeImpl) Shutdown(done core.Closure) {

}

func (ni *NodeImpl) Join() {

}

func (ni *NodeImpl) Apply(task *entity.Task) {

}

func (ni *NodeImpl) ReadIndex(reqCtx []byte, done *core.ReadIndexClosure) {

}

func (ni *NodeImpl) ListPeers() []*entity.PeerId {

}

func (ni *NodeImpl) ListAlivePeers() []*entity.PeerId {

}

func (ni *NodeImpl) ListLearners() []*entity.PeerId {

}

func (ni *NodeImpl) ListAliceLearners() []*entity.PeerId {

}

func (ni *NodeImpl) AddPeer(peer *entity.PeerId, done core.Closure) {

}

func (ni *NodeImpl) RemovePeer(peer *entity.PeerId, done core.Closure) {

}

func (ni *NodeImpl) ChangePeers(newConf *entity.Configuration, done core.Closure) {

}

func (ni *NodeImpl) ResetPeers(newConf *entity.Configuration) entity.Status {

}

func (ni *NodeImpl) AddLearners(learners []*entity.PeerId, done core.Closure) {

}

func (ni *NodeImpl) RemoveLearners(learners []*entity.PeerId, done core.Closure) {

}

func (ni *NodeImpl) ResetLearners(learners []*entity.PeerId, done core.Closure) {

}

func (ni *NodeImpl) Snapshot(done core.Closure) {

}

func (ni *NodeImpl) ResetElectionTimeoutMs(electionTimeoutMs int32) {

}

func (ni *NodeImpl) TransferLeadershipTo(peer *entity.PeerId) entity.Status {

}

func (ni *NodeImpl) ReadCommittedUserLog(index int64) *entity.UserLog {

}

func (ni *NodeImpl) AddReplicatorStateListener(replicatorStateListener replicate.ReplicatorStateListener) {

}

func (ni *NodeImpl) RemoveReplicatorStateListener(replicatorStateListener replicate.ReplicatorStateListener) {

}

func (ni *NodeImpl) ClearReplicatorStateListeners() {

}

func (ni *NodeImpl) GetReplicatorStatueListeners() []replicate.ReplicatorStateListener {

}

func (ni *NodeImpl) GetNodeTargetPriority() int32 {

}

func (ni *NodeImpl) OnError(err rafterror.RaftError) {

}

func (ni *NodeImpl) IsCurrentLeaderValid() bool {
	return utils.GetCurrentTimeMs()-ni.lastLeaderTimestamp < ni.options.ElectionTimeoutMs
}

type LeaderStableClosure struct {
	core.StableClosure
	node *NodeImpl
}

func (lsc *LeaderStableClosure) Run(status entity.Status) {
	node := lsc.node
	if status.IsOK() {
		node.ballotBox.CommitAt(node.firstLogIndex, node.firstLogIndex+int64(node.nEntries)-1, node.serverID)
	} else {
		node.logger.Error("Node %s append [%d, %d] failed, status=%+v.", node.nodeID.GetDesc(), node.firstLogIndex, node.firstLogIndex+int64(node.nEntries)-1, status)
	}
}

type readIndexHeartbeatResponseClosure struct {
	core.RpcResponseClosure
	readIndexResp		*core.ReadIndexResponse
	closure            *core.RpcResponseClosure
	quorum             int32
	failPeersThreshold int32
	ackSuccess         int32
	ackFailures        int32
	isDone             bool
}

func (rhc *readIndexHeartbeatResponseClosure) Run(status entity.Status)  {
	if rhc.isDone {
		return
	}
	if status.IsOK() && rhc.Resp.(*core.AppendEntriesResponse).Success {
		rhc.ackSuccess ++
	} else {
		rhc.ackFailures ++
	}
	utils.RequireNonNil(rhc.readIndexResp, "ReadIndexResponse")
	if rhc.ackSuccess + 1 >= rhc.quorum {
		rhc.readIndexResp.Success = true
	} else if rhc.ackFailures >= rhc.failPeersThreshold {
		rhc.readIndexResp.Success = false
	}
	rhc.closure.Resp = rhc.readIndexResp
	rhc.closure.Run(entity.StatusOK())
	rhc.isDone = true
}

type raftRpcHandler struct {
	node *NodeImpl
}

func (rrh *raftRpcHandler) handlePreVoteRequest() func(input payload.Payload, req proto.Message, sink mono.Sink) {
	return func(input payload.Payload, req proto.Message, sink mono.Sink) {
		node := rrh.node
		doUnLock := true
		defer func() {
			if doUnLock {
				node.rwMutex.Unlock()
			}
		}()
		node.rwMutex.Lock()

		preVoteReq := req.(*core.RequestVoteRequest)
		if !IsNodeActive(node.state) {
			node.logger.Warn("Node %s is not in active state, currTerm=%d.", node.nodeID.GetDesc(), node.currTerm)
			voteResp := &core.RequestVoteResponse{
				Term:          0,
				Granted:       false,
				ErrorResponse: core.NewErrorResponse(entity.EINVAL, "Node %s is not in active state, state %s.", node.nodeID.GetDesc(), node.state.GetName()),
			}
			rrh.monoSink(voteResp, sink)
			return
		}

		candidateId := &entity.PeerId{}
		if !candidateId.Parse(preVoteReq.ServerID) {
			node.logger.Warn("Node %s received PreVoteRequest from %s serverId bad format.", node.nodeID.GetDesc(), preVoteReq.ServerID)
			voteResp := &core.RequestVoteResponse{
				Term:          0,
				Granted:       false,
				ErrorResponse: core.NewErrorResponse(entity.EINVAL, "Parse candidateId failed: %s.", preVoteReq.ServerID),
			}
			rrh.monoSink(voteResp, sink)
			return
		}
		granted := false
		for {
			if node.leaderID != nil && !node.leaderID.IsEmpty() && node.IsCurrentLeaderValid() {
				node.logger.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d, because the leader %s's lease is still valid.",
					node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm, node.leaderID.GetDesc())
				break
			}
			if preVoteReq.Term < node.currTerm {
				node.logger.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d.", node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm)
				rrh.checkReplicator(candidateId)
				break
			} else if preVoteReq.Term == node.currTerm + 1 {
				rrh.checkReplicator(candidateId)
			}
			doUnLock = false
			node.rwMutex.Unlock()

			lastLogID := node.logManager.GetLastLogID(true)
			doUnLock = true
			node.rwMutex.Lock()
			requestLastLogId := entity.NewLogID(preVoteReq.LastLogIndex, preVoteReq.LastLogTerm)
			granted = requestLastLogId.Compare(lastLogID) >= 0
			if false {
				break
			}
		}
		preVoteResp := &core.RequestVoteResponse{
			Term:    node.currTerm,
			Granted: granted,
		}
		rrh.monoSink(preVoteResp, sink)
	}
}

func (rrh *raftRpcHandler) checkReplicator(candidate *entity.PeerId) {
	
}

func (rrh *raftRpcHandler) monoSink(resp proto.Message, sink mono.Sink) {
	body, err := ptypes.MarshalAny(resp)
	if err != nil {
		sink.Error(err)
		return
	}
	gRPCResp := &transport.GrpcResponse{
		Body: body,
	}
	result, err := proto.Marshal(gRPCResp)
	if err != nil {
		sink.Error(err)
		return
	}
	sink.Success(payload.New(result, rpc.EmptyBytes))
}

func (rrh *raftRpcHandler) fluxSink(resp proto.Message, sink flux.Sink) {

}
