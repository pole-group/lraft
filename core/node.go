// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	pole_rpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"

	proto2 "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/utils"
)

type Task struct {
	Done       Closure
	ExpectTerm int64
	Data       []byte
}

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
	return "LogEntryAndClosure"
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
	GetLeaderID() entity.PeerId

	GetNodeID() entity.NodeId

	GetGroupID() string

	GetOptions() NodeOptions

	GetRaftOptions() RaftOptions

	IsLeader() bool

	Shutdown(done Closure)

	Join()

	Apply(task *Task) error

	ReadIndex(reqCtx []byte, done *ReadIndexClosure) error

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

/*

 */
const (
	StateLeader        NodeState = iota // It's a leader
	StateTransferring                   // It's transferring leadership
	StateCandidate                      // It's a candidate
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
	rwMutex             *sync.RWMutex
	state               NodeState
	groupID             string
	currTerm            int64
	firstLogIndex       int64
	nEntries            int32
	lastLeaderTimestamp int64
	raftNodeJobMgn      *RaftNodeJobManager
	fsmCaller           FSMCaller
	targetPriority      int32
	nodeID              *entity.NodeId
	serverID            entity.PeerId
	leaderID            entity.PeerId
	options             *NodeOptions
	raftOptions         RaftOptions
	readOnlyOperator    *ReadOnlyOperator
	conf                entity.ConfigurationEntry
	voteCtx             *entity.Ballot
	preVoteCtx          *entity.Ballot
	ballotBox           *BallotBox
	handler             *raftRpcHandler
	replicatorGroup     *ReplicatorGroup
	logManager          LogManager
	snapshotExecutor    SnapshotExecutor
	rpcServer           *rpc.RaftRPCServer
	shutdownWait        *sync.WaitGroup
}

func (node *nodeImpl) GetLeaderID() entity.PeerId {
	defer node.rwMutex.RUnlock()
	node.rwMutex.RLock()
	if node.leaderID.IsEmpty() {
		return entity.EmptyPeer
	}
	return node.leaderID
}

func (node *nodeImpl) GetNodeID() *entity.NodeId {
	if node.nodeID == nil {
		node.nodeID = &entity.NodeId{
			GroupID: node.GetGroupID(),
			Peer:    node.serverID,
		}
	}
	return node.nodeID
}

func (node *nodeImpl) GetGroupID() string {
	return node.groupID
}

func (node *nodeImpl) GetOptions() NodeOptions {
	return *node.options
}

func (node *nodeImpl) GetRaftOptions() RaftOptions {
	return node.raftOptions
}

func (node *nodeImpl) IsLeader() bool {
	return node.IsLeaderWithBLock(true)
}

func (node *nodeImpl) IsLeaderWithBLock(blocking bool) bool {
	if !blocking {
		return node.state == StateLeader
	}
	defer node.rwMutex.RUnlock()
	node.rwMutex.RLock()
	return node.state == StateLeader
}

func (node *nodeImpl) Shutdown(done Closure) {

}

func (node *nodeImpl) Join() {

}

func (node *nodeImpl) Apply(task *Task) error {
	if node.shutdownWait != nil {
		task.Done.Run(entity.NewStatus(entity.ENodeShutdown, "Node is shutting down."))
		return fmt.Errorf("node is shutting down")
	}
	if _, err := utils.RequireNonNil(task, "nil task"); err != nil {
		return err
	}

	entry := &entity.LogEntry{}
	entry.Data = task.Data

	retryCnt := 3
	for i := 0; i < retryCnt; i++ {
		success, err := utils.PublishEventNonBlock(&LogEntryAndClosure{
			Entry:        entry,
			Done:         task.Done,
			ExpectedTerm: task.ExpectTerm,
			Latch:        nil,
		})
		if err != nil {
			return err
		}
		if success {
			return nil
		}
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	utils.RaftLog.Warn("node is busy, has too many tasks")
	return fmt.Errorf("node is busy, has too many tasks")
}

func (node *nodeImpl) ReadIndex(reqCtx []byte, done *ReadIndexClosure) error {
	if node.shutdownWait != nil {
		done.Run(entity.NewStatus(entity.ENodeShutdown, "Node is shutting down."))
		return fmt.Errorf("node is shutting down")
	}
	if _, err := utils.RequireNonNil(done, "nil closure"); err != nil {
		return err
	}
	node.readOnlyOperator.addRequest(reqCtx, done)
	return nil
}

func (node *nodeImpl) ListPeers() []*entity.PeerId {

}

func (node *nodeImpl) ListAlivePeers() []*entity.PeerId {

}

func (node *nodeImpl) ListLearners() []*entity.PeerId {

}

func (node *nodeImpl) ListAliceLearners() []*entity.PeerId {

}

func (node *nodeImpl) AddPeer(peer *entity.PeerId, done Closure) {

}

func (node *nodeImpl) RemovePeer(peer *entity.PeerId, done Closure) {

}

func (node *nodeImpl) ChangePeers(newConf *entity.Configuration, done Closure) {

}

func (node *nodeImpl) ResetPeers(newConf *entity.Configuration) entity.Status {

}

func (node *nodeImpl) AddLearners(learners []*entity.PeerId, done Closure) {

}

func (node *nodeImpl) RemoveLearners(learners []*entity.PeerId, done Closure) {

}

func (node *nodeImpl) ResetLearners(learners []*entity.PeerId, done Closure) {

}

func (node *nodeImpl) Snapshot(done Closure) {

}

func (node *nodeImpl) ResetElectionTimeoutMs(electionTimeoutMs int32) {

}

func (node *nodeImpl) TransferLeadershipTo(peer *entity.PeerId) entity.Status {

}

func (node *nodeImpl) ReadCommittedUserLog(index int64) *entity.UserLog {

}

func (node *nodeImpl) AddReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (node *nodeImpl) RemoveReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (node *nodeImpl) ClearReplicatorStateListeners() {

}

func (node *nodeImpl) GetReplicatorStatueListeners() []ReplicatorStateListener {

}

func (node *nodeImpl) GetNodeTargetPriority() int32 {

}

func (node *nodeImpl) OnError(err entity.RaftError) {

}

func (node *nodeImpl) IsCurrentLeaderValid() bool {
	return utils.GetCurrentTimeMs()-node.lastLeaderTimestamp < node.options.ElectionTimeoutMs
}

func (node *nodeImpl) IsisLeaderLeaseValid() bool {
	nowTime := time.Now()
	if node.checkLeaderLease(nowTime) {
		return true
	}
	node.checkDeadNodes0(node.conf.GetConf().GetPeers(), nowTime, false, nil)
	return node.checkLeaderLease(nowTime)
}

func (node *nodeImpl) checkLeaderLease(t time.Time) bool {
	return t.Unix()-node.lastLeaderTimestamp < int64(node.options.LeaderLeaseTimeRatio)
}

func (node *nodeImpl) checkDeadNodes0(peers *utils.Set, monotonicNowMs time.Time, checkReplicator bool, deadNodes *entity.Configuration) {

}

//resetLeaderId
//1、释放当前的 LeaderID 信息, 即自己没有收到来自 Leader 的心跳信息之后, 自己的状态不再为 Follower, 因此会触发一个 StopFollow 的回调
//2、通知当前新的 LeaderID 信息
func (node *nodeImpl) resetLeaderId(newLeaderId entity.PeerId, status entity.Status) {
	if newLeaderId.IsEmpty() {
		if !node.leaderID.IsEmpty() && node.state > StateTransferring {
			node.fsmCaller.OnStopFollowing(entity.LeaderChangeContext{
				LeaderID: node.leaderID.Copy(),
				Term:     node.currTerm,
				Status:   status,
			})
		}
		node.leaderID = entity.EmptyPeer
	} else {
		if node.leaderID.IsEmpty() {
			node.fsmCaller.OnStartFollowing(entity.LeaderChangeContext{
				LeaderID: newLeaderId,
				Term:     node.currTerm,
				Status:   status,
			})
		}
		node.leaderID = newLeaderId.Copy()
	}
}

func (node *nodeImpl) GetQuorum() int {
	c := node.conf.GetConf()
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
		utils.RaftLog.Error("Node %s append [%d, %d] failed, status=%#v.", node.nodeID.GetDesc(),
			node.firstLogIndex, node.firstLogIndex+int64(node.nEntries)-1, status)
	}
}

type raftRpcHandler struct {
	node *nodeImpl
}

func (rrh *raftRpcHandler) init() {
	rrh.node.rpcServer.GetRealServer().RegisterRequestHandler(rpc.CoreRequestPreVoteRequest, rrh.handlePreVoteRequest())
}

func (rrh *raftRpcHandler) handlePreVoteRequest() func(cxt context.Context,
	rpcCtx pole_rpc.RpcServerContext) {
	return func(cxt context.Context, rpcCtx pole_rpc.RpcServerContext) {
		node := rrh.node
		doUnLock := true
		defer func() {
			if doUnLock {
				node.rwMutex.Unlock()
			}
		}()
		node.rwMutex.Lock()

		preVoteReq := &proto2.RequestVoteRequest{}
		if err := ptypes.UnmarshalAny(rpcCtx.GetReq().Body, preVoteReq); err != nil {
			panic(err)
		}

		if !IsNodeActive(node.state) {
			utils.RaftLog.Warn("Node %s is not in active state, currTerm=%d.", node.nodeID.GetDesc(), node.currTerm)
			voteResp := &proto2.RequestVoteResponse{
				Term:    0,
				Granted: false,
				ErrorResponse: entity.NewErrorResponse(entity.EINVAL, "Node %s is not in active state, state %s.",
					node.nodeID.GetDesc(), node.state.GetName()),
			}
			resp, err := rrh.convertToGrpcResp(voteResp)
			if err != nil {
				panic(err)
			}
			rpcCtx.Send(resp)
			return
		}

		candidateId := &entity.PeerId{}
		if !candidateId.Parse(preVoteReq.ServerID) {
			utils.RaftLog.Warn("Node %s received PreVoteRequest from %s serverId bad format.",
				node.nodeID.GetDesc(), preVoteReq.ServerID)
			voteResp := &proto2.RequestVoteResponse{
				Term:          0,
				Granted:       false,
				ErrorResponse: entity.NewErrorResponse(entity.EINVAL, "Parse candidateId failed: %s.", preVoteReq.ServerID),
			}
			resp, err := rrh.convertToGrpcResp(voteResp)
			if err != nil {
				panic(err)
			}
			rpcCtx.Send(resp)
			return
		}
		granted := false
		for {
			if !node.leaderID.IsEmpty() && node.IsCurrentLeaderValid() {
				utils.RaftLog.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d, "+
					"because the leader %s's lease is still valid.",
					node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm, node.leaderID.GetDesc())
				break
			}
			if preVoteReq.Term < node.currTerm {
				utils.RaftLog.Info("Node %s ignore PreVoteRequest from %s, term=%d, currTerm=%d.", node.nodeID.GetDesc(), preVoteReq.ServerID, preVoteReq.Term, node.currTerm)
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
		resp, err := rrh.convertToGrpcResp(preVoteResp)
		if err != nil {
			panic(err)
		}
		rpcCtx.Send(resp)
		return
	}
}

func (rrh *raftRpcHandler) checkReplicator(candidate *entity.PeerId) {

}

func (rrh *raftRpcHandler) convertToGrpcResp(resp proto.Message) (*pole_rpc.ServerResponse, error) {
	body, err := ptypes.MarshalAny(resp)
	if err != nil {
		return nil, err
	}
	gRPCResp := &pole_rpc.ServerResponse{
		Body: body,
	}
	return gRPCResp, nil
}
