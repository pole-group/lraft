// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lraft

import (
	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
)

type Options func(opt *Option)

type Option struct {
	GroupID  string
	ServerID entity.PeerId
	NodeOpt  *core.NodeOptions
}

//NewRaftNode 创建一个 Raft 节点
func NewRaftNode(options ...Options) core.Node {
	return nil
}
