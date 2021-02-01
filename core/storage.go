// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import "github.com/pole-group/lraft/entity"

type RaftMetaStorage struct {
	node     *nodeImpl
	term     int64
	path     string
	voteFor  entity.PeerId
	raftOpts RaftOptions
	isInited bool
}

// setTermAndVotedFor 
func (rms *RaftMetaStorage) setTermAndVotedFor(term int64, peer entity.PeerId) {

}

func (rms *RaftMetaStorage) reportIOError() {

}

func (rms *RaftMetaStorage) save() {

}
