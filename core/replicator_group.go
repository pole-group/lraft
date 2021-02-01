// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"fmt"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/utils"
)

type ReplicatorType string

const (
	ReplicatorFollower ReplicatorType = "Follower" // 可以参与投票的 Follower
	ReplicatorLearner  ReplicatorType = "Learner"  // 仅仅参与日志复制的 Learner，可以用来当作容灾的节点或者只读节点
)

func (rt ReplicatorType) IsFollower() bool {
	return rt == ReplicatorFollower
}

func (rt ReplicatorType) IsLearner() bool {
	return rt == ReplicatorLearner
}

type ReplicatorGroup struct {
	replicators        *utils.ConcurrentMap // <string, *Replicator>
	raftOpt            RaftOptions
	commonOptions      *replicatorOptions
	failureReplicators *utils.ConcurrentMap // <string, ReplicatorType>
}

func (rpg *ReplicatorGroup) checkReplicator(peer entity.PeerId, lockNode bool) {
	replicator := rpg.GetReplicator(peer)
	if replicator == nil {
		node := rpg.commonOptions.node
		defer func() {
			if lockNode {
				node.lock.Unlock()
			}
		}()
		if lockNode {
			node.lock.Lock()
		}
		if node.IsLeader() {
			rType := rpg.failureReplicators.Get(peer.GetDesc())
			if rType != nil {
				if ok, _ := rpg.AddReplicator(peer, rType.(ReplicatorType), false); ok {
					rpg.failureReplicators.Remove(peer.GetDesc())
				}
			}
		}
	}
}

func (rpg *ReplicatorGroup) clearFailureReplicators() {
	rpg.failureReplicators.Clear()
}

func (rpg *ReplicatorGroup) transferLeadershipTo(peer entity.PeerId, lastLogIndex int64) (bool, error) {
	return false, nil
}

//sendHeartbeat
func (rpg *ReplicatorGroup) sendHeartbeat(peer entity.PeerId, closure *AppendEntriesResponseClosure) {
	replicator := rpg.GetReplicator(peer)
	if replicator == nil {
		if closure != nil {
			closure.Run(entity.NewStatus(entity.EHostDown, fmt.Sprintf("peer %s is not connected", peer.GetDesc())))
		}
		return
	}
	replicator.sendHeartbeat(closure)
}

func (rpg *ReplicatorGroup) resetTerm(term int64)  {
	// TODO
}

//GetReplicator
func (rpg *ReplicatorGroup) GetReplicator(peer entity.PeerId) *Replicator {
	return rpg.replicators.Get(peer.GetDesc()).(*Replicator)
}

//AddReplicator 添加一个复制者
func (rpg *ReplicatorGroup) AddReplicator(peer entity.PeerId, replicatorType ReplicatorType, sync bool) (bool, error) {
	if err := utils.RequireTrue(rpg.commonOptions.term != 0, "term is zero"); err != nil {
		return false, err
	}
	rpg.failureReplicators.Remove(peer.GetDesc())
	if rpg.replicators.Contains(peer.GetDesc()) {
		return true, nil
	}
	// 判断是否需要重新新建一个 replicatorOptions
	opts := utils.IF(rpg.commonOptions == nil, &replicatorOptions{}, rpg.commonOptions.Copy()).(*replicatorOptions)
	opts.replicatorType = replicatorType
	opts.peerId = peer
	if !sync {
		if ok, err := opts.raftRpcOperator.raftClient.CheckConnection(peer.GetEndpoint()); !ok || err != nil {
			utils.RaftLog.Error("Fail to check replicator connection to peer=%s, replicatorType=%s.", peer.GetDesc(),
				replicatorType)
			rpg.failureReplicators.Put(peer.GetDesc(), peer)
			return false, err
		}
	}

	replicator := NewReplicator(opts, rpg.raftOpt)
	if ok, err := replicator.Start(); !ok || err != nil {
		utils.RaftLog.Error("fail to startJob replicator to peer=%s, replicatorType=%", peer.GetDesc(), replicatorType)
		return false, err
	}

	rpg.replicators.Put(peer.GetDesc(), replicator)
	return true, nil
}

func (rpg *ReplicatorGroup) stopAllAndFindTheNextCandidate(conf *entity.ConfigurationEntry) *Replicator {
	var replicator *Replicator
	candidateId := rpg.findTheNextCandidate(conf)
	if !candidateId.IsEmpty() {
		replicator = rpg.replicators.Get(candidateId.GetDesc()).(*Replicator)
	} else {
		utils.RaftLog.Info("fail to find the next candidate.")
	}
	rpg.replicators.ForEach(func(k, v interface{}) {
		r := v.(*Replicator)
		if r != replicator {
			r.shutdown()
		}
	})
	rpg.replicators.Clear()
	rpg.failureReplicators.Clear()
	return replicator
}

func (rpg *ReplicatorGroup) findTheNextCandidate(conf *entity.ConfigurationEntry) entity.PeerId {
	peer := entity.EmptyPeer
	priority := entity.ElectionPriorityMin
	maxIndex := int64(-1)
	rpg.replicators.ForEach(func(k, v interface{}) {
		p := entity.PeerId{}
		p.Parse(k.(string))
		if !conf.ContainPeer(p) {
			return
		}
		nextPriority := p.GetPriority()
		if nextPriority == entity.ElectionPriorityNotElected {
			return
		}
		replicator := v.(*Replicator)
		nextIndex := replicator.nextIndex
		if nextIndex > maxIndex {
			maxIndex = nextIndex
			peer = p
			priority = p.GetPriority()
		} else if nextIndex == maxIndex && nextPriority > priority {
			peer = p
			priority = p.GetPriority()
		}
	})

	if maxIndex == -1 {
		return entity.EmptyPeer
	}
	return peer
}

func (rpg *ReplicatorGroup) sendTimeoutNowAndStop(replicator *Replicator, electionTimeoutMs int64) {

}

func (rpg *ReplicatorGroup) stopAll() {

}
