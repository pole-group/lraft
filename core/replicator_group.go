package core

import (
	"fmt"
	"sync"

	"github.com/pole-group/lraft/entity"
)

type ReplicatorType string

const (
	ReplicatorFollower ReplicatorType = "Follower"
	ReplicatorLeader   ReplicatorType = "Leader"
)

type ReplicatorGroup struct {
	lock               sync.RWMutex
	replicators        map[*entity.PeerId]*Replicator
	raftOpt            RaftOptions
	failureReplicators map[*entity.PeerId]ReplicatorType
}

func (rpg *ReplicatorGroup) SendHeartbeat(peer *entity.PeerId, done *AppendEntriesResponseClosure)  {
	rpg.lock.RLock()
	replicator := rpg.replicators[peer]
	rpg.lock.RUnlock()

	if replicator == nil {
		if done != nil {
			done.Run(entity.NewStatus(entity.EHostDown, fmt.Sprintf("peer %s is not connected", peer.GetDesc())))
		}
		return
	}
	replicator.SendHeartbeat(done)
}
