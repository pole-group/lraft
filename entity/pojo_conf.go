// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package entity

import (
	"container/list"

	"github.com/pole-group/lraft/utils"
)

const (
	LearnersPostfix = "/learner"
)

type Configuration struct {
	peers    *utils.Set
	learners *utils.Set
}

func NewEmptyConfiguration() *Configuration {
	return &Configuration{}
}

func NewConfiguration(peers, learners []PeerId) *Configuration {
	var _peers []PeerId
	var _learners []PeerId

	for _, e := range peers {
		_peers = append(_peers, e.Copy())
	}

	for _, e := range learners {
		_learners = append(_learners, e.Copy())
	}

	return &Configuration{
		peers:    utils.NewSetWithValues(_peers),
		learners: utils.NewSetWithValues(_learners),
	}
}

func (c *Configuration) AddPeers(peers []PeerId) {
	c.peers.AddAll(peers)
}

func (c *Configuration) SetPeers(peers []PeerId) {
	c.peers = utils.NewSetWithValues(peers)
}

func (c *Configuration) GetPeers() *utils.Set {
	return c.peers
}

func (c *Configuration) Contains(id PeerId) bool {
	return c.peers.Contain(id)
}

func (c *Configuration) ListPeers() []PeerId {
	ids := make([]PeerId, 0)
	c.peers.ToSlice(ids)
	return ids
}

func (c *Configuration) RemovePeer(peer *PeerId) {
	c.peers.Remove(peer)
}

func (c *Configuration) GetLearners() *utils.Set {
	return c.learners
}

func (c *Configuration) SetLearners(learners []PeerId) {
	c.learners = utils.NewSetWithValues(learners)
}

func (c *Configuration) AddLearners(learners []PeerId) {
	c.learners.AddAll(learners)
}

func (c *Configuration) ListLearners() []PeerId {
	ids := make([]PeerId, 0)
	c.learners.ToSlice(ids)
	return ids
}

func (c *Configuration) RemoveLearners(peer *PeerId) {
	c.learners.Remove(peer)
}

func (c *Configuration) Copy() *Configuration {
	return NewConfiguration(c.ListPeers(), c.ListLearners())
}

func (c *Configuration) IsValid() bool {
	intersection := utils.NewSetWithValues(c.peers)
	intersection.RetainAll(c.learners)
	return c.peers.Size() != 0 && intersection.IsEmpty()
}

func (c *Configuration) Rest() {
	c.peers = utils.NewSet()
	c.learners = utils.NewSet()
}

func (c *Configuration) IsEmpty() bool {
	return c.peers.Size() == 0
}

func (c *Configuration) Size() int {
	return c.peers.Size()
}

func (c *Configuration) Diff(rhs, included, excluded *Configuration) {
	included.peers = utils.NewSet()
	included.peers.RemoveAllWithSet(rhs.peers)
	excluded.peers = utils.NewSet()
	excluded.peers.RetainAllWithSet(rhs.peers)
}

type ConfigurationEntry struct {
	id      LogId
	conf    *Configuration
	oldConf *Configuration
}

func NewEmptyConfigurationEntry() *ConfigurationEntry {
	return &ConfigurationEntry{}
}

func NewConfigurationEntry(id LogId, conf, oldConf *Configuration) *ConfigurationEntry {
	return &ConfigurationEntry{
		id:      id,
		conf:    conf,
		oldConf: oldConf,
	}
}

func (ce *ConfigurationEntry) GetID() LogId {
	return ce.id
}

func (ce *ConfigurationEntry) GetConf() *Configuration {
	return ce.conf
}

func (ce *ConfigurationEntry) GetOldConf() *Configuration {
	return ce.oldConf
}

func (ce *ConfigurationEntry) SetID(id LogId) {
	ce.id = id
}

func (ce *ConfigurationEntry) SetConf(conf *Configuration) {
	ce.conf = conf
}

func (ce *ConfigurationEntry) SetOldConf(oldConf *Configuration) {
	ce.oldConf = oldConf
}

func (ce *ConfigurationEntry) IsStable() bool {
	return ce.oldConf.IsEmpty()
}

func (ce *ConfigurationEntry) IsValid() bool {
	if !ce.conf.IsValid() {
		return false
	}

	intersection := ce.ListPeers()
	intersection.RetainAllWithSet(ce.ListLearners())
	if intersection.IsEmpty() {
		return true
	}
	// TODO LOG
	return false
}

func (ce *ConfigurationEntry) IsEmpty() bool {
	return ce.conf == nil || ce.conf.IsEmpty()
}

func (ce *ConfigurationEntry) ContainPeer(p PeerId) bool {
	return ce.conf.Contains(p) || ce.oldConf.Contains(p)
}

func (ce *ConfigurationEntry) ContainLearner(l PeerId) bool {
	return ce.conf.GetLearners().Contain(l) || ce.oldConf.GetLearners().Contain(l)
}

func (ce *ConfigurationEntry) ListPeers() *utils.Set {
	s := utils.NewSet()
	s.AddAllWithSet(ce.conf.GetPeers())
	s.AddAllWithSet(ce.conf.GetPeers())
	return s
}

func (ce *ConfigurationEntry) ListLearners() *utils.Set {
	s := utils.NewSet()
	s.AddAllWithSet(ce.conf.GetLearners())
	s.AddAllWithSet(ce.conf.GetLearners())
	return s
}

type ConfigurationManager struct {
	configurations list.List
	snapshot       *ConfigurationEntry
}

func (cm *ConfigurationManager) Add(ce *ConfigurationEntry) bool {
	if cm.configurations.Len() != 0 {
		if cm.configurations.Front().Value.(*ConfigurationEntry).GetID().GetIndex() >= ce.GetID().GetIndex() {
			// TODO Log
			return false
		}
	}
	cm.configurations.PushFront(ce)
	return true
}

//TruncatePrefix 删除在这之前的所有数据
func (cm *ConfigurationManager) TruncatePrefix(firstIndexKept int64) {
	l := cm.configurations
	for {
		if l.Len() != 0 && l.Front().Value.(*ConfigurationEntry).GetID().GetIndex() < firstIndexKept {
			l.Remove(l.Front())
			continue
		}
		break
	}
	cm.configurations = l
}

//TruncateSuffix 删除在这之后的所有数据
func (cm *ConfigurationManager) TruncateSuffix(lastIndexKept int64) {
	l := cm.configurations
	for {
		if l.Len() != 0 && l.Front().Value.(*ConfigurationEntry).GetID().GetIndex() > lastIndexKept {
			l.Remove(l.Back())
			continue
		}
		break
	}
	cm.configurations = l
}

func (cm *ConfigurationManager) GetSnapshot() *ConfigurationEntry {
	return cm.snapshot
}

func (cm *ConfigurationManager) SetSnapshot(snapshot *ConfigurationEntry) {
	cm.snapshot = snapshot
}

func (cm *ConfigurationManager) GetLastConfiguration() *ConfigurationEntry {
	if cm.configurations.Len() == 0 {
		return cm.snapshot
	}
	return cm.configurations.Front().Value.(*ConfigurationEntry)
}

func (cm *ConfigurationManager) Get(lastIncludedIndex int64) (*ConfigurationEntry, error) {
	l := cm.configurations
	if l.Len() == 0 {
		if err := utils.RequireTrue(lastIncludedIndex >= cm.snapshot.GetID().GetIndex(),
			"lastIncludedIndex %d is less than snapshot index %d", lastIncludedIndex,
			cm.snapshot.GetID().GetIndex()); err != nil {
			return nil, err
		}
	}

	e := l.Front()

	for e != nil && e.Next() != nil {
		n := e.Next()

		if n.Value.(*ConfigurationEntry).GetID().GetIndex() > lastIncludedIndex {
			e = e.Prev()
			break
		}
		e = e.Next()
	}

	if e != nil && e.Prev() != nil {
		return e.Prev().Value.(*ConfigurationEntry), nil
	}
	return cm.snapshot, nil
}
