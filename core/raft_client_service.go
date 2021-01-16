package core

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/mono"
	"github.com/panjf2000/ants/v2"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
	"github.com/pole-group/lraft/transport"
)

// RaftClient 的一些操作
type RaftClientOperator struct {
	raftClient     *rpc.RaftClient
	replicateGroup *ReplicatorGroup
	nodeOpt        *NodeOptions
	endpointGoPool map[string]*ants.PoolWithFunc
}

func NewRaftClientOperator(nodeOpt *NodeOptions) *RaftClientOperator {

}

func (rcop *RaftClientOperator) AppendEntries(endpoint entity.Endpoint,
	req *proto.AppendEntriesRequest, done *AppendEntriesResponseClosure) mono.Mono {
	body, err := ptypes.MarshalAny(req)
	if err != nil {
		return mono.Error(err)
	}

	gRep := &transport.GrpcRequest{
		Label: rpc.CoreAppendEntriesRequest,
		Body:  body,
	}

	return invokeWithClosure(endpoint, rcop.raftClient, gRep, done)
}

func (rcop *RaftClientOperator) RequestVote(endpoint entity.Endpoint, req *proto.RequestVoteRequest) mono.Mono {

}

func (rcop *RaftClientOperator) PreVote(endpoint entity.Endpoint, req *proto.RequestVoteRequest,
	done *RpcResponseClosure) mono.Mono {

	body, err := ptypes.MarshalAny(req)
	if err != nil {
		return mono.Error(err)
	}

	gRep := &transport.GrpcRequest{
		Label: rpc.CoreRequestVoteRequest,
		Body:  body,
	}

	return invokeWithClosure(endpoint, rcop.raftClient, gRep, done)
}

func (rcop *RaftClientOperator) InstallSnapshot(endpoint entity.Endpoint, req *proto.InstallSnapshotRequest) mono.Mono {

}

func (rcop *RaftClientOperator) ReadIndex(endpoint entity.Endpoint, req *proto.ReadIndexRequest,
	done *ReadIndexResponseClosure) mono.Mono {

}

func (rcop *RaftClientOperator) TimeoutNow(endpoint entity.Endpoint, req *proto.TimeoutNowRequest,
	done Closure) mono.Mono {

}

func invokeWithClosure(endpoint entity.Endpoint, rpcClient *rpc.RaftClient, req *transport.GrpcRequest,
	done Closure) mono.Mono {
	future, err := rpcClient.SendRequest(endpoint, req)

	if err != nil {
		future = mono.Error(err)
	}

	return future.DoOnNext(func(v reactor.Any) error {
		resp := v.(*transport.GrpcResponse)
		supplier := rpc.GlobalProtoRegistry.FindProtoMessageSupplier(resp.Label)
		bzResp := supplier()
		if err := ptypes.UnmarshalAny(resp.Body, supplier()); err != nil {
			return err
		}
		done.(*RpcResponseClosure).Resp = bzResp
		done.Run(entity.StatusOK())
		return nil
	}).DoOnCancel(func() {
		done.Run(entity.NewStatus(entity.ECANCELED, "RPC request was canceled by future."))
	}).DoOnError(func(e error) {
		done.Run(entity.NewStatus(entity.UNKNOWN, e.Error()))
	})
}
