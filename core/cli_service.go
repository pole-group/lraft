// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/rpc"
)

type CliService struct {
	timeoutMs int32
	maxRetry  int32
	rpcClient *rpc.RaftClient
}

func (cli *CliService) AddPeer(groupId string, peerId *entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) RemovePeer(groupId string, peerId *entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) ChangePeer(groupId string, oldConf, newConf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) ResetPeer(groupId string, peerId *entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) AddLearners(groupId string, learners []*entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) RemoveLearners(groupId string, learners []*entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) ResetLearners(groupId string, learners []*entity.PeerId,
	conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) TransferLeader(groupId string, peerId *entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) Snapshot(groupId string, peerId *entity.PeerId) entity.Status {
	return entity.Status{}
}

func (cli *CliService) GetLeader(groupId string, leaderId *entity.PeerId, conf *entity.Configuration) entity.Status {
	return entity.Status{}
}

func (cli *CliService) GetPeers(groupId string, peerId *entity.PeerId, conf *entity.Configuration) []*entity.PeerId {
	return nil
}

func (cli *CliService) GetAlivePeers(groupId string, peerId *entity.PeerId, conf *entity.Configuration) []*entity.PeerId {
	return nil
}

func (cli *CliService) GetLearners(groupId string, peerId *entity.PeerId, conf *entity.Configuration) []*entity.PeerId {
	return nil
}

func (cli *CliService) GetAliveLearners(groupId string, peerId *entity.PeerId, conf *entity.Configuration) []*entity.PeerId {
	return nil
}

func (cli *CliService) ReBalance(groupIds []string, balanceLeaderIds map[string]*entity.PeerId, conf *entity.Configuration) []*entity.PeerId {
	return nil
}
