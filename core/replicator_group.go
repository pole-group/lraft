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
	ReplicatorFollower ReplicatorType = "Follower"
	ReplicatorLeader   ReplicatorType = "Leader"
)

type ReplicatorGroup struct {
	replicators        *utils.ConcurrentMap // <string, *Replicator>
	raftOpt            RaftOptions
	commonOptions      *replicatorOptions
	failureReplicators *utils.ConcurrentMap // <string, ReplicatorType>
}

func (rpg *ReplicatorGroup) SendHeartbeat(peer entity.PeerId, done *AppendEntriesResponseClosure) {
	replicator := rpg.GetReplicator(peer)

	if replicator == nil {
		if done != nil {
			done.Run(entity.NewStatus(entity.EHostDown, fmt.Sprintf("peer %s is not connected", peer.GetDesc())))
		}
		return
	}
	replicator.SendHeartbeat(done)
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
	replicator.SendHeartbeat(closure)
}

//GetReplicator
func (rpg *ReplicatorGroup) GetReplicator(peer entity.PeerId) *Replicator {
	return rpg.replicators.Get(peer.GetDesc()).(*Replicator)
}

//AddReplicator
func (rpg *ReplicatorGroup) AddReplicator(peer entity.PeerId, replicatorType ReplicatorType, sync bool) (bool, error) {
	if err := utils.RequireTrue(rpg.commonOptions.term != 0, "term is zero"); err != nil {
		return false, err
	}
	rpg.failureReplicators.Remove(peer.GetDesc())
	if rpg.replicators.Contains(peer.GetDesc()) {
		return true, nil
	}
	opts := utils.IF(rpg.commonOptions == nil, &replicatorOptions{}, rpg.commonOptions.Copy()).(*replicatorOptions)
	opts.replicatorType = replicatorType
	opts.peerId = peer
	if !sync {
		opts.raftRpcOperator.raftClient.CheckConnection(peer.GetEndpoint())
	}
	return true, nil
}
