// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/mono"
	polerpc "github.com/pole-group/pole-rpc"

	raft "github.com/pole-group/lraft/proto"
)

const (
	// cli command
	// 添加一个学习者的命令
	CliAddLearnerRequest string = "CliAddLearnerCommand"
	// 添加一个成员的命令
	CliAddPeerRequest        string = "CliAddPeerCommand"
	// 更改成员的命令
	CliChangePeersRequest    string = "CliChangePeersCommand"
	// 获取 Leader 的命令
	CliGetLeaderRequest      string = "CliGetLeaderCommand"
	// 获取所有的成员
	CliGetPeersRequest       string = "CliGetPeersCommand"
	// 批量移除学习者
	CliRemoveLearnersRequest string = "CliRemoveLearnersCommand"
	CliResetLearnersRequest  string = "CliResetLearnersCommand"
	CliResetPeersRequest     string = "CliResetPeersCommand"
	CliSnapshotRequest       string = "CliSnapshotCommand"
	CliTransferLeaderRequest string = "CliTransferLeaderCommand"

	// proto command
	CoreAppendEntriesRequest   string = "CoreAppendEntriesCommand"
	CoreGetFileRequest         string = "CoreGetFileCommand"
	CoreInstallSnapshotRequest string = "CoreInstallSnapshotCommand"
	CoreNodeRequest            string = "CoreNodeCommand"
	CoreReadIndexRequest       string = "CoreReadIndexCommand"
	CoreRequestPreVoteRequest  string = "CoreRequestPreVoteRequest"
	CoreRequestVoteRequest     string = "CoreRequestVoteCommand"
	CoreTimeoutNowRequest      string = "CoreTimeoutNowCommand"

	//
	CommonRpcErrorCommand string = "CommonRpcErrorCommand"
)

//RequestIDKey 每次请求的唯一标识
const RequestIDKey string = "RequestID"

//RPCContext 请求上下文
type RPCContext struct {
	onceSink mono.Sink
	manySink flux.Sink
	Values   map[interface{}]interface{}
}

//NewRPCCtx 创建一个新的 RPCContext
func NewRPCCtx() *RPCContext {
	return &RPCContext{
		Values: make(map[interface{}]interface{}),
	}
}

//SendMsg 发送一个响应
func (c *RPCContext) SendMsg(msg *polerpc.ServerResponse) {
	reqID := c.Value(RequestIDKey).(string)
	msg.RequestId = reqID
	if c.onceSink != nil {
		c.onceSink.Success(msg)
	}
	if c.manySink != nil {
		c.manySink.Next(msg)
	}
}

//Write 追加一个 attachment 到 RPCContext
func (c *RPCContext) Write(key, value interface{}) {
	c.Values[key] = value
}

//Value 获取上下文的 attachment
func (c *RPCContext) Value(key interface{}) interface{} {
	return c.Values[key]
}

//Close 关闭一个Rpc请求上下文
func (c *RPCContext) Close() {
	if c.manySink != nil {
		c.manySink.Complete()
	}
}

//GlobalProtoRegistry 全局的 proto.Message supplier 的仓库
var GlobalProtoRegistry *ProtobufMessageRegistry

//ProtobufMessageRegistry 用于自动化的将字节数组转换为业务结构体
type ProtobufMessageRegistry struct {
	rwLock   sync.RWMutex
	registry map[string]func() proto.Message
}

//RegistryProtoMessageSupplier 注册一个 proto.Message 对象构造者
func (pmr *ProtobufMessageRegistry) RegistryProtoMessageSupplier(key string, supplier func() proto.Message) bool {
	defer pmr.rwLock.Unlock()
	pmr.rwLock.Lock()
	if _, exist := pmr.registry[key]; !exist {
		pmr.registry[key] = supplier
		return true
	}
	return false
}

//FindProtoMessageSupplier 根据名称找到 proto.Message 的对象构造者，每次调用对象构造者，会创建出一个新的 proto.Message
func (pmr *ProtobufMessageRegistry) FindProtoMessageSupplier(key string) func() proto.Message {
	defer pmr.rwLock.RUnlock()
	pmr.rwLock.RLock()
	return pmr.registry[key]
}

func init() {
	GlobalProtoRegistry = &ProtobufMessageRegistry{
		rwLock:   sync.RWMutex{},
		registry: make(map[string]func() proto.Message),
	}

	// cli 模块
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliAddLearnerRequest, func() proto.Message {
		return &raft.AddLearnersRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliAddPeerRequest, func() proto.Message {
		return &raft.AddPeerRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliChangePeersRequest, func() proto.Message {
		return &raft.ChangePeersRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliGetLeaderRequest, func() proto.Message {
		return &raft.GetLeaderRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliGetPeersRequest, func() proto.Message {
		return &raft.GetPeersRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliRemoveLearnersRequest, func() proto.Message {
		return &raft.RemoveLearnersRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliResetLearnersRequest, func() proto.Message {
		return &raft.ResetLearnersRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliResetPeersRequest, func() proto.Message {
		return &raft.ResetPeerRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliSnapshotRequest, func() proto.Message {
		return &raft.SnapshotRequest{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CliTransferLeaderRequest, func() proto.Message {
		return &raft.TransferLeaderRequest{}
	})

	// proto 模块
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreAppendEntriesRequest, func() proto.Message {
		return &raft.AddPeerResponse{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreGetFileRequest, func() proto.Message {
		return &raft.GetFileResponse{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreInstallSnapshotRequest, func() proto.Message {
		return &raft.InstallSnapshotResponse{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreReadIndexRequest, func() proto.Message {
		return &raft.ReadIndexResponse{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreRequestVoteRequest, func() proto.Message {
		return &raft.RequestVoteResponse{}
	})
	GlobalProtoRegistry.RegistryProtoMessageSupplier(CoreTimeoutNowRequest, func() proto.Message {
		return &raft.TimeoutNowResponse{}
	})

}
