// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package lraft

import (
	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/core"
	"github.com/pole-group/lraft/entity"
)

type Options func(opt *Option)

type Option struct {
	GroupID  string
	ServerID entity.PeerId
	NodeOpt  *core.NodeOptions
}

func init() {
	polerpc.DefaultScheduler = polerpc.NewRoutinePool(256, 128)
}

//NewRaftNode 创建一个 Raft 节点
func NewRaftNode(options ...Options) core.Node {
	return nil
}
