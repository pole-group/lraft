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
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/mono"
	polerpc "github.com/pole-group/pole-rpc"

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
	newPeers    []entity.PeerId
	oldPeers    []entity.PeerId
	addingPeers []entity.PeerId

	learners    []entity.PeerId
	oldLearners []entity.PeerId
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

func (cc *ConfigurationCtx) flush(newConf, oldConf *entity.Configuration) {

}

func (cc *ConfigurationCtx) Reset() {

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

	ListPeers() ([]entity.PeerId, error)

	ListAlivePeers() ([]entity.PeerId, error)

	ListLearners() ([]entity.PeerId, error)

	ListAliveLearners() ([]entity.PeerId, error)

	AddPeer(peer entity.PeerId, done Closure)

	RemovePeer(peer entity.PeerId, done Closure)

	ChangePeers(newConf *entity.Configuration, done Closure)

	ResetPeers(newConf *entity.Configuration) entity.Status

	AddLearners(learners []entity.PeerId, done Closure)

	RemoveLearners(learners []entity.PeerId, done Closure)

	ResetLearners(learners []entity.PeerId, done Closure)

	Snapshot(done Closure)

	ResetElectionTimeoutMs(electionTimeoutMs int32)

	TransferLeadershipTo(peer entity.PeerId) entity.Status

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
	lock                     *sync.RWMutex
	state                    NodeState
	groupID                  string
	currTerm                 int64
	firstLogIndex            int64
	nEntries                 int32
	lastLeaderTimestamp      int64
	raftNodeJobMgn           *RaftNodeJobManager
	fsmCaller                FSMCaller
	targetPriority           int32
	nodeID                   entity.NodeId
	serverID                 entity.PeerId
	leaderID                 entity.PeerId
	votedId                  entity.PeerId
	options                  NodeOptions
	raftOptions              RaftOptions
	readOnlyOperator         *ReadOnlyOperator
	confCtx                  *ConfigurationCtx
	conf                     *entity.ConfigurationEntry
	voteCtx                  *entity.Ballot
	preVoteCtx               *entity.Ballot
	ballotBox                *BallotBox
	handler                  *raftRpcHandler
	replicatorGroup          *ReplicatorGroup
	logManager               LogManager
	metaStorage              *RaftMetaStorage
	snapshotExecutor         *SnapshotExecutor
	rpcServer                *rpc.RaftRPCServer
	shutdownWait             *sync.WaitGroup
	raftOperator             *RaftClientOperator
	replicatorStateListeners []ReplicatorStateListener
	transferFuture           polerpc.Future
	wakingCandidate          *Replicator
	stopTransferArg          *stopTransferArg
}

func (node *nodeImpl) init() {
	node.lock.Lock()
	if node.conf.IsStable() && node.conf.GetConf().Size() == 1 && node.conf.ContainPeer(node.serverID) {
		electSelf(node)
	} else {
		node.lock.Unlock()
	}
}

func (node *nodeImpl) GetLeaderID() entity.PeerId {
	defer node.lock.RUnlock()
	node.lock.RLock()
	if node.leaderID.IsEmpty() {
		return entity.EmptyPeer
	}
	return node.leaderID
}

func (node *nodeImpl) GetNodeID() entity.NodeId {
	if entity.IsEmptyNodeID(node.nodeID) {
		node.nodeID = entity.NodeId{
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
	return node.options
}

func (node *nodeImpl) GetRaftOptions() RaftOptions {
	return node.raftOptions
}

func (node *nodeImpl) IsLeader() bool {
	return node.IsLeaderWithBLock(true)
}

func (node *nodeImpl) IsLearner() bool {
	return false
}

func (node *nodeImpl) IsLeaderWithBLock(blocking bool) bool {
	if !blocking {
		return node.state == StateLeader
	}
	defer node.lock.RUnlock()
	node.lock.RLock()
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

func (node *nodeImpl) ListPeers() ([]entity.PeerId, error) {
	defer node.lock.RUnlock()
	node.lock.Lock()
	if node.state != StateLeader {
		return nil, fmt.Errorf("not leader")
	}
	return node.conf.GetConf().ListPeers(), nil
}

func (node *nodeImpl) ListAlivePeers() ([]entity.PeerId, error) {
	defer node.lock.RUnlock()
	node.lock.Lock()
	if node.state != StateLeader {
		return nil, fmt.Errorf("not leader")
	}
	return node.getAlivePeers(node.conf.GetConf().ListPeers(), utils.GetCurrentTimeMs()), nil
}

func (node *nodeImpl) ListLearners() ([]entity.PeerId, error) {
	defer node.lock.RUnlock()
	node.lock.Lock()
	if node.state != StateLeader {
		return nil, fmt.Errorf("not leader")
	}
	return node.conf.GetConf().ListLearners(), nil
}

func (node *nodeImpl) ListAliveLearners() ([]entity.PeerId, error) {
	defer node.lock.RUnlock()
	node.lock.Lock()
	if node.state != StateLeader {
		return nil, fmt.Errorf("not leader")
	}
	return node.getAlivePeers(node.conf.GetConf().ListLearners(), utils.GetCurrentTimeMs()), nil
}

func (node *nodeImpl) AddPeer(peer entity.PeerId, done Closure) {

}

func (node *nodeImpl) RemovePeer(peer entity.PeerId, done Closure) {

}

func (node *nodeImpl) ChangePeers(newConf *entity.Configuration, done Closure) {

}

func (node *nodeImpl) ResetPeers(newConf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (node *nodeImpl) AddLearners(learners []entity.PeerId, done Closure) {

}

func (node *nodeImpl) RemoveLearners(learners []entity.PeerId, done Closure) {

}

func (node *nodeImpl) ResetLearners(learners []entity.PeerId, done Closure) {

}

func (node *nodeImpl) Snapshot(done Closure) {

}

func (node *nodeImpl) ResetElectionTimeoutMs(electionTimeoutMs int32) {

}

func (node *nodeImpl) TransferLeadershipTo(peer entity.PeerId) entity.Status {
	if peer.IsEmpty() {
		return entity.NewStatus(entity.ERequest, "peer is empty")
	}

	if peer.GetIP() == utils.IPAny {
		return entity.NewStatus(entity.ERequest, "illegal peer")
	}

	if peer.Equal(node.serverID) {
		return entity.StatusOK()
	}

	if !node.conf.ContainPeer(peer) {
		return entity.NewStatus(entity.EINVAL, fmt.Sprintf("peer %s not in current configuration", peer.GetDesc()))
	}

	defer node.lock.Unlock()
	node.lock.Lock()

	if node.state != StateLeader {
		utils.RaftLog.Warn("node %s can't transfer leadership to peer %s as it is in state %s.",
			node.nodeID.GetDesc(), peer.GetDesc(), node.state.GetName())
		return entity.NewStatus(utils.IF(node.state == StateTransferring, entity.EBUSY,
			entity.EPERM).(entity.RaftErrorCode), "not a leader")
	}
	if node.confCtx.IsBusy() {
		utils.RaftLog.Warn("Node %s refused to transfer leadership to peer %s when the leader is changing the"+
			" configuration.", node.nodeID.GetDesc(), peer.GetDesc())
		return entity.NewStatus(entity.EBUSY, "changing the configuration")
	}

	lastLogIndex := node.logManager.GetLastLogIndex()
	if ok, err := node.replicatorGroup.transferLeadershipTo(peer, lastLogIndex); !ok || err != nil {
		utils.RaftLog.Warn("no such peer : %s", peer.GetDesc())
		return entity.NewStatus(entity.EINVAL, "no such peer "+peer.GetDesc())
	}

	node.state = StateTransferring
	st := entity.NewStatus(entity.ETransferLeaderShip, fmt.Sprintf("raft leader is transferring leadership to %s",
		peer.GetDesc()))
	node.onLeaderStop(st)
	arg := stopTransferArg{
		term: node.currTerm,
		peer: peer,
	}
	node.transferFuture = polerpc.NewMonoFuture(mono.
		Delay(time.Duration(node.options.ElectionTimeoutMs) * time.Millisecond).
		DoOnNext(
			func(v reactor.Any) error {
				node.onTransferTimeout(arg)
				return nil
			}))
	return entity.StatusOK()
}

func (node *nodeImpl) AddReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (node *nodeImpl) RemoveReplicatorStateListener(replicatorStateListener ReplicatorStateListener) {

}

func (node *nodeImpl) ClearReplicatorStateListeners() {

}

func (node *nodeImpl) GetReplicatorStatueListeners() []ReplicatorStateListener {
	return node.replicatorStateListeners
}

func (node *nodeImpl) GetNodeTargetPriority() int32 {
	return node.targetPriority
}

//onError 发生了不可挽回的异常信息，需要停止本节点的所有任务工作
func (node *nodeImpl) onError(err entity.RaftError) {
	utils.RaftLog.Error("node %s got error: %#v.", node.nodeID.GetDesc(), err)
	if node.fsmCaller != nil {
		// 通知用户状态机，当前RaftGroupNode出现了无法挽救的异常，需要及时处理
		node.fsmCaller.OnError(err)
	}
	if node.readOnlyOperator != nil {
		// 所有的 RaftReadIndex 都不能继续工作
		node.readOnlyOperator.err = &err
	}
	defer node.lock.Unlock()
	node.lock.Lock()
	if node.state <= StateFollower {
		stepDown(node, node.currTerm, node.state == StateLeader, entity.NewStatus(entity.EBadNode, "Raft node(leader or candidate) is in error."))
	}
	if node.state <= StateError {
		node.state = StateError
	}
}

//getAlivePeers 获取 Leader 认为存活的节点数据，此方法只能由 Leader 节点进行调用
func (node *nodeImpl) getAlivePeers(peers []entity.PeerId, monotonicNowMs int64) []entity.PeerId {
	leaderLeaseTimeoutMs := node.options.getLeaderLeaseTimeoutMs()
	newPeers := make([]entity.PeerId, 0, 0)
	for _, peer := range peers {
		if peer.Equal(node.serverID) || monotonicNowMs-node.replicatorGroup.getReplicator(peer).lastRpcSendTimestamp <= leaderLeaseTimeoutMs {
			newPeers = append(newPeers, peer.Copy())
		}
	}
	return newPeers
}

func (node *nodeImpl) getLeaderLeaseTimeoutMs() int64 {
	return node.options.ElectionTimeoutMs * int64(node.options.LeaderLeaseTimeRatio) / 100
}

func (node *nodeImpl) currentLeaderIsValid() bool {
	return utils.GetCurrentTimeMs()-node.lastLeaderTimestamp < node.options.ElectionTimeoutMs
}

func (node *nodeImpl) leaderLeaseIsValid() bool {
	nowTime := time.Now()
	if node.checkLeaderLease(nowTime) {
		return true
	}
	node.checkDeadNodes0(node.conf.GetConf().ListPeers(), nowTime, false, nil)
	return node.checkLeaderLease(nowTime)
}

func (node *nodeImpl) checkLeaderLease(t time.Time) bool {
	return t.Unix()*1000-node.lastLeaderTimestamp < int64(node.options.LeaderLeaseTimeRatio)
}

//checkDeadNodes 检查当前 RaftGroup 集群是否可以正常对外工作
func (node *nodeImpl) checkDeadNodes(conf *entity.Configuration, monotonicNowMs time.Time, stepDownOnCheckFail bool) bool {
	for _, peer := range conf.ListLearners() {
		node.replicatorGroup.checkReplicator(peer, false)
	}
	peers := conf.ListPeers()
	deadNodes := entity.NewEmptyConfiguration()
	if node.checkDeadNodes0(peers, monotonicNowMs, true, deadNodes) {
		return true
	}
	if stepDownOnCheckFail {
		stepDown(node, node.currTerm, false, entity.NewStatus(entity.ERaftTimedOut,
			fmt.Sprintf("majority of the group dies: %d/%d", deadNodes.Size(), len(peers))))
	}
	return false
}

//checkDeadNodes0 检查当前 Raft Node 是否可以用
func (node *nodeImpl) checkDeadNodes0(peers []entity.PeerId, monotonicNowMs time.Time, checkReplicator bool,
	deadNodes *entity.Configuration) bool {
	leaderLeaseTimeoutMs := node.getLeaderLeaseTimeoutMs()
	aliveCount := 0
	startLease := utils.Int64MaxValue

	ms := monotonicNowMs.Unix() * 1000

	for _, peer := range peers {
		if peer.Equal(node.serverID) {
			continue
		}
		if checkReplicator {
			node.replicatorGroup.checkReplicator(peer, false)
		}
		lastRpcSendTimestamp := node.replicatorGroup.getReplicator(peer).lastRpcSendTimestamp
		if ms-lastRpcSendTimestamp <= leaderLeaseTimeoutMs {
			aliveCount++
			if startLease > lastRpcSendTimestamp {
				startLease = lastRpcSendTimestamp
			}
			continue
		}
		if deadNodes != nil {
			deadNodes.AddPeers(peers)
		}
	}
	if aliveCount >= len(peers)/2+1 {
		node.lastLeaderTimestamp = startLease
		return true
	}
	return false
}

//onTransferTimeout 当移交 Leader 角色超时时的处理
func (node *nodeImpl) onTransferTimeout(arg stopTransferArg) {
	node.handleTransferTimeout(arg.term, arg.peer)
}

//handleTransferTimeout 处理转移 Leader 任务超时的逻辑处理
func (node *nodeImpl) handleTransferTimeout(term int64, peer entity.PeerId) {
	utils.RaftLog.Info("node %s failed to transfer leadership to peer %s, reached timeout.", node.nodeID.GetDesc(),
		peer.GetDesc())
	defer node.lock.Unlock()
	node.lock.Lock()
	if term == node.currTerm {
		//TODO 为什么任务超时直接强制将 Leader 指定为自己
		node.replicatorGroup.stopTransferLeadership(peer)
		if node.state == StateTransferring {
			node.fsmCaller.OnLeaderStart(term)
			node.state = StateLeader
			node.stopTransferArg = nil
		}
	}
}

//stepDown 节点降级
func (node *nodeImpl) stepDown(term int64, wakeupCandidate bool) {
	utils.RaftLog.Warn("node %s stepDown, term=%s, newTerm=%s, wakeupCandidate=%s.", node.nodeID.GetDesc(),
		node.currTerm, term, wakeupCandidate)
	if !IsNodeActive(node.state) {
		return
	}
	if node.state == StateCandidate {
		node.raftNodeJobMgn.stopJob(JobForVote)
	}
}

//onLeaderStop
func (node *nodeImpl) onLeaderStop(st entity.Status) {
	node.replicatorGroup.clearFailureReplicators()
	node.fsmCaller.OnLeaderStop(st)
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

type stopTransferArg struct {
	term int64
	peer entity.PeerId
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
	rpcCtx polerpc.RpcServerContext) {
	return func(cxt context.Context, rpcCtx polerpc.RpcServerContext) {
		node := rrh.node
		doUnLock := true
		defer func() {
			if doUnLock {
				node.lock.Unlock()
			}
		}()
		node.lock.Lock()

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

		candidateId := entity.PeerId{}
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
			if !node.leaderID.IsEmpty() && node.currentLeaderIsValid() {
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
			node.lock.Unlock()

			lastLogID := node.logManager.GetLastLogID(true)
			doUnLock = true
			node.lock.Lock()
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

func (rrh *raftRpcHandler) checkReplicator(candidate entity.PeerId) {

}

func (rrh *raftRpcHandler) convertToGrpcResp(resp proto.Message) (*polerpc.ServerResponse, error) {
	body, err := ptypes.MarshalAny(resp)
	if err != nil {
		return nil, err
	}
	gRPCResp := &polerpc.ServerResponse{
		Body: body,
	}
	return gRPCResp, nil
}
