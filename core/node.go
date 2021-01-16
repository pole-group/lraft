package core

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go/mono"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"

	"github.com/pole-group/lraft/entity"

	log "github.com/pole-group/lraft/logger"
	proto2 "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/transport"
	"github.com/pole-group/lraft/utils"
)

type LogEntryAndClosure struct {
	Entry        *entity.LogEntry
	Done         Closure
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
	node        *nodeImpl
	stage       Stage
	nChanges    int32
	version     int64
	newPeers    []*entity.PeerId
	oldPeers    []*entity.PeerId
	addingPeers []*entity.PeerId

	learners    []*entity.PeerId
	oldLearners []*entity.PeerId
	done        Closure
}

func NewConfigurationCtx(node *nodeImpl) *ConfigurationCtx {
	return &ConfigurationCtx{
		node:    node,
		stage:   StageNone,
		version: 0,
	}
}

func (cc *ConfigurationCtx) Start(conf, oldConf *entity.Configuration, done Closure) {

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

	Shutdown(done Closure)

	Join()

	Apply(task *entity.Task)

	ReadIndex(reqCtx []byte, done *ReadIndexClosure)

	ListPeers() []*entity.PeerId

	ListAlivePeers() []*entity.PeerId

	ListLearners() []*entity.PeerId

	ListAliceLearners() []*entity.PeerId

	AddPeer(peer *entity.PeerId, done Closure)

	RemovePeer(peer *entity.PeerId, done Closure)

	ChangePeers(newConf *entity.Configuration, done Closure)

	ResetPeers(newConf *entity.Configuration) entity.Status

	AddLearners(learners []*entity.PeerId, done Closure)

	RemoveLearners(learners []*entity.PeerId, done Closure)

	ResetLearners(learners []*entity.PeerId, done Closure)

	Snapshot(done Closure)

	ResetElectionTimeoutMs(electionTimeoutMs int32)

	TransferLeadershipTo(peer *entity.PeerId) entity.Status

	ReadCommittedUserLog(index int64) *entity.UserLog

	AddReplicatorStateListener(replicatorStateListener ReplicatorStateListener)

	RemoveReplicatorStateListener(replicatorStateListener ReplicatorStateListener)

	ClearReplicatorStateListeners()

	GetReplicatorStatueListeners() []ReplicatorStateListener

	GetNodeTargetPriority() int32
}

type NodeState int

const (
	StateLeader        NodeState = iota // It's a leader
	StateTransferring                   // It's transferring leadership
	StateCandidate                      //  It's a candidate
	StateFollower                       // It's a follower
	StateError                          // It's in error
	StateUninitialized                  // It's uninitialized
	StateShutting                       // It's shutting down
	StateShutdown                       // It's shutdown already
	StateEnd                            // State end
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

type nodeImpl struct {
	rwMutex             sync.RWMutex
	state               NodeState
	nodeID              entity.NodeId
	currTerm            int64
	leaderID            *entity.PeerId
	firstLogIndex       int64
	nEntries            int32
	lastLeaderTimestamp int64
	options             NodeOptions
	raftOptions         RaftOptions
	conf                entity.ConfigurationEntry
	voteCtx             *entity.Ballot
	preVoteCtx          *entity.Ballot
	ballotBox           *BallotBox
	serverID            *entity.PeerId
	handler             *raftRpcHandler
	replicatorGroup     *ReplicatorGroup
	logManager          LogManager
}

func (ni *nodeImpl) GetLeaderID() *entity.PeerId {

}

func (ni *nodeImpl) GetNodeID() *entity.NodeId {

}

func (ni *nodeImpl) GetGroupID() string {

}

func (ni *nodeImpl) GetOptions() NodeOptions {

}

func (ni *nodeImpl) GetRaftOptions() RaftOptions {

}

func (ni *nodeImpl) IsLeader() bool {

}

func (ni *nodeImpl) Shutdown(done Closure) {

}

func (ni *nodeImpl) Join() {

}

func (ni *nodeImpl) Apply(task *entity.Task) {

}

func (ni *nodeImpl) ReadIndex(reqCtx []byte, done *ReadIndexClosure) {

}

func (ni *nodeImpl) ListPeers() []*entity.PeerId {

}

func (ni *nodeImpl) ListAlivePeers() []*entity.PeerId {

}

func (ni *nodeImpl) ListLearners() []*entity.PeerId {

}

func (ni *nodeImpl) ListAliceLearners() []*entity.PeerId {

}

func (ni *nodeImpl) AddPeer(peer *entity.PeerId, done Closure) {

}

func (ni *nodeImpl) RemovePeer(peer *entity.PeerId, done Closure) {

}

func (ni *nodeImpl) ChangePeers(newConf *entity.Configuration, done Closure) {

}

func (ni *nodeImpl) ResetPeers(newConf *entity.Configuration) entity.Status {

}

func (ni *nodeImpl) AddLearners(learners []*entity.PeerId, done Closure) {

}

func (ni *nodeImpl) RemoveLearners(learners []*entity.PeerId, done Closure) {

}

func (ni *nodeImpl) ResetLearners(learners []*entity.PeerId, done Closure) {

}

func (ni *nodeImpl) Snapshot(done Closure) {

}

func (ni *nodeImpl) ResetElectionTimeoutMs(electionTimeoutMs int32) {

}

func (ni *nodeImpl) TransferLeadershipTo(peer *entity.PeerId) entity.Status {

}

func (ni *nodeImpl) ReadCommittedUserLog(index int64) *entity.UserLog {

}

func (ni *nodeImpl) AddReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (ni *nodeImpl) RemoveReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (ni *nodeImpl) ClearReplicatorStateListeners() {

}

func (ni *nodeImpl) GetReplicatorStatueListeners() []ReplicatorStateListener {

}

func (ni *nodeImpl) GetNodeTargetPriority() int32 {

}

func (ni *nodeImpl) OnError(err entity.RaftError) {

}

func (ni *nodeImpl) IsCurrentLeaderValid() bool {
	return utils.GetCurrentTimeMs()-ni.lastLeaderTimestamp < ni.options.ElectionTimeoutMs
}

func (ni *nodeImpl) IsisLeaderLeaseValid() bool {
	nowTime := time.Now()
	if ni.checkLeaderLease(nowTime) {
		return true
	}
	ni.checkDeadNodes0(ni.conf.GetConf().GetPeers(), nowTime, false, nil)
	return ni.checkLeaderLease(nowTime)
}

func (ni *nodeImpl) checkLeaderLease(t time.Time) bool {
	return t.Unix()-ni.lastLeaderTimestamp < int64(ni.options.LeaderLeaseTimeRatio)
}

func (ni *nodeImpl) checkDeadNodes0(peers *utils.Set, monotonicNowMs time.Time, checkReplicator bool, deadNodes *entity.Configuration) {

}

func (ni *nodeImpl) GetQuorum() int {
	c := ni.conf.GetConf()
	if c.IsEmpty() {
		return 0
	}
	return c.GetPeers().Size()/2 + 1
}

type LeaderStableClosure struct {
	StableClosure
	node *nodeImpl
}

func (lsc *LeaderStableClosure) Run(status entity.Status) {
	node := lsc.node
	if status.IsOK() {
		node.ballotBox.CommitAt(node.firstLogIndex, node.firstLogIndex+int64(node.nEntries)-1, node.serverID)
	} else {
		log.GlobalRaftLog.Error("Node %s append [%d, %d] failed, status=%+v.", node.nodeID.GetDesc(), node.firstLogIndex, node.firstLogIndex+int64(node.nEntries)-1, status)
	}
}

type readIndexHeartbeatResponseClosure struct {
	AppendEntriesResponseClosure
	readIndexResp      *proto2.ReadIndexResponse
	closure            *ReadIndexResponseClosure
	quorum             int32
	failPeersThreshold int32
	ackSuccess         int32
	ackFailures        int32
	isDone             bool
}

//NewReadIndexHeartbeatResponseClosure readIndexResp 不涉及网络传输，根据从 Leader 返回的 AppendEntriesResponse 信息决定 readIndexResp
//的内容是什么
func NewReadIndexHeartbeatResponseClosure(done *ReadIndexResponseClosure, readIndexResp *proto2.ReadIndexResponse, quorum, peerSize int32) *readIndexHeartbeatResponseClosure {
	rhc := &readIndexHeartbeatResponseClosure{
		readIndexResp:      readIndexResp,
		closure:            done,
		quorum:             quorum,
		failPeersThreshold: utils.IF(peerSize%2 == 0, quorum-1, quorum).(int32),
		ackSuccess:         0,
		ackFailures:        0,
		isDone:             false,
	}

	rhc.AppendEntriesResponseClosure = AppendEntriesResponseClosure{
		RpcResponseClosure{
			F: func(resp proto.Message, status entity.Status) {
				if rhc.isDone {
					return
				}
				if status.IsOK() && resp.(*proto2.AppendEntriesResponse).Success {
					rhc.ackSuccess++
				} else {
					rhc.ackFailures++
				}
				_, err := utils.RequireNonNil(rhc.readIndexResp, "ReadIndexResponse")
				if err != nil {
					rhc.closure.Run(entity.NewStatus(entity.EInternal, err.Error()))
					return
				}
				if rhc.ackSuccess+1 >= rhc.quorum {
					rhc.readIndexResp.Success = true
				} else if rhc.ackFailures >= rhc.failPeersThreshold {
					rhc.readIndexResp.Success = false
				}
				rhc.closure.Resp = rhc.readIndexResp
				rhc.closure.Run(entity.StatusOK())
				rhc.isDone = true
			},
		},
	}

	return rhc
}

func (rhc *readIndexHeartbeatResponseClosure) Run(status entity.Status) {
	rhc.RpcResponseClosure.Run(status)
}

type raftRpcHandler struct {
	node *nodeImpl
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

		preVoteReq := req.(*proto2.RequestVoteRequest)
		if !IsNodeActive(node.state) {
			log.GlobalRaftLog.Warn("Node %s is not in active state, currTerm=%d.", node.nodeID.GetDesc(), node.currTerm)
			voteResp := &proto2.RequestVoteResponse{
				Term:          0,
				Granted:       false,
				ErrorResponse: utils.NewErrorResponse(entity.EINVAL, "Node %s is not in active state, state %s.", node.nodeID.GetDesc(), node.state.GetName()),
			}
			rrh.monoSink(voteResp, sink)
			return
		}

		candidateId := &entity.PeerId{}
		if !candidateId.Parse(preVoteReq.ServerID) {
			log.GlobalRaftLog.Warn("Node %s received PreVoteRequest from %s serverId bad format.",
				node.nodeID.GetDesc(), preVoteReq.ServerID)
			voteResp := &proto2.RequestVoteResponse{
				Term:          0,
				Granted:       false,
				ErrorResponse: utils.NewErrorResponse(entity.EINVAL, "Parse candidateId failed: %s.", preVoteReq.ServerID),
			}
			rrh.monoSink(voteResp, sink)
			return
		}
		granted := false
		for {
			if node.leaderID != nil && !node.leaderID.IsEmpty() && node.IsCurrentLeaderValid() {
				log.GlobalRaftLog.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d, "+
					"because the leader %s's lease is still valid.",
					node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm, node.leaderID.GetDesc())
				break
			}
			if preVoteReq.Term < node.currTerm {
				log.GlobalRaftLog.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d.", node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm)
				rrh.checkReplicator(candidateId)
				break
			} else if preVoteReq.Term == node.currTerm+1 {
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
		preVoteResp := &proto2.RequestVoteResponse{
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
