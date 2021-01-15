package rpc

import (
	"errors"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/mono"

	raft "github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/transport"
)

const (
	// cli command
	CliAddLearnerRequest     string = "CliAddLearnerCommand"
	CliAddPeerRequest        string = "CliAddPeerCommand"
	CliChangePeersRequest    string = "CliChangePeersCommand"
	CliGetLeaderRequest      string = "CliGetLeaderCommand"
	CliGetPeersRequest       string = "CliGetPeersCommand"
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
	CoreRequestVoteRequest     string = "CoreRequestVoteCommand"
	CoreTimeoutNowRequest      string = "CoreTimeoutNowCommand"

	//
	CommonRpcErrorCommand string = "CommonRpcErrorCommand"
)

var (
	EmptyBytes     []byte = make([]byte, 0)
	ServerNotFount        = errors.New("target server not found")
)

const RequestIDKey string = "RequestID"

type RpcContext struct {
	onceSink mono.Sink
	manySink flux.Sink
	Values   map[interface{}]interface{}
}

func NewCtxPole() *RpcContext {
	return &RpcContext{
		Values: make(map[interface{}]interface{}),
	}
}

func (c *RpcContext) SendMsg(msg *transport.GrpcResponse) {
	reqId := c.Value("RequestIDKey").(string)
	msg.RequestId = reqId
	if c.onceSink != nil {
		c.onceSink.Success(msg)
	}
	if c.manySink != nil {
		c.manySink.Next(msg)
	}
}

func (c *RpcContext) Write(key, value interface{}) {
	c.Values[key] = value
}

func (c *RpcContext) Value(key interface{}) interface{} {
	return c.Values[key]
}

func (c *RpcContext) Close() {
	if c.manySink != nil {
		c.manySink.Complete()
	}
}

var GlobalProtoRegistry *ProtobufMessageRegistry

type ProtobufMessageRegistry struct {
	rwLock   sync.RWMutex
	registry map[string]func() proto.Message
}

func (pmr *ProtobufMessageRegistry) RegistryProtoMessageSupplier(key string, supplier func() proto.Message) bool {
	defer pmr.rwLock.Unlock()
	pmr.rwLock.Lock()
	if _, exist := pmr.registry[key]; !exist {
		pmr.registry[key] = supplier
		return true
	}
	return false
}

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
