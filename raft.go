// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lraft

import (
	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
)

//NewRaftNode 创建一个 Raft 节点
func NewRaftNode(groupID string, serverID *entity.PeerId, opts *core.NodeOptions) core.Node {
	return nil
}
