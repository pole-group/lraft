// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package core

import (
	proto2 "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/mono"
	"github.com/panjf2000/ants/v2"
	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/proto"
	"github.com/pole-group/lraft/rpc"
)

// RaftClient 的一些操作
type RaftClientOperator struct {
	raftClient     *rpc.RaftClient
	replicateGroup *ReplicatorGroup
	nodeOpt        *NodeOptions
	endpointGoPool map[string]*ants.PoolWithFunc
}

func NewRaftClientOperator(nodeOpt *NodeOptions, raftClient *rpc.RaftClient, replicateGroup *ReplicatorGroup) *RaftClientOperator {
	return &RaftClientOperator{
		raftClient:     raftClient,
		replicateGroup: replicateGroup,
		nodeOpt:        nodeOpt,
		endpointGoPool: make(map[string]*ants.PoolWithFunc),
	}
}

func (rcop *RaftClientOperator) AppendEntries(endpoint entity.Endpoint,
	req *proto.AppendEntriesRequest, done *AppendEntriesResponseClosure) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreAppendEntriesRequest, req, &done.RpcResponseClosure)
}

func (rcop *RaftClientOperator) RequestVote(endpoint entity.Endpoint, req *proto.RequestVoteRequest,
	done *OnRequestVoteRpcDone) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreRequestVoteRequest, req, &done.RpcResponseClosure)
}

func (rcop *RaftClientOperator) PreVote(endpoint entity.Endpoint, req *proto.RequestVoteRequest,
	done *OnPreVoteRpcDone) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreRequestPreVoteRequest, req, &done.RpcResponseClosure)
}

func (rcop *RaftClientOperator) InstallSnapshot(endpoint entity.Endpoint, req *proto.InstallSnapshotRequest,
	done *InstallSnapshotResponseClosure) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreInstallSnapshotRequest, req, &done.RpcResponseClosure)
}

func (rcop *RaftClientOperator) ReadIndex(endpoint entity.Endpoint, req *proto.ReadIndexRequest,
	done *ReadIndexResponseClosure) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreReadIndexRequest, req, &done.RpcResponseClosure)
}

func (rcop *RaftClientOperator) TimeoutNow(endpoint entity.Endpoint, req *proto.TimeoutNowRequest,
	done *TimeoutNowResponseClosure) mono.Mono {
	return invokeWithClosure(endpoint, rcop.raftClient, rpc.CoreTimeoutNowRequest, req, &done.RpcResponseClosure)
}

func invokeWithClosure(endpoint entity.Endpoint, rpcClient *rpc.RaftClient, path string, req proto2.Message,
	done *RpcResponseClosure) mono.Mono {
	body, err := ptypes.MarshalAny(req)
	if err != nil {
		return mono.Error(err)
	}

	gRep := &polerpc.ServerRequest{
		Body:    body,
		FunName: path,
	}

	resp, err := rpcClient.SendRequest(endpoint, gRep)
	if err != nil {
		return mono.Error(err)
	}

	return mono.Just(resp).DoOnNext(func(v reactor.Any) error {
		resp := v.(*polerpc.ServerResponse)
		supplier := rpc.GlobalProtoRegistry.FindProtoMessageSupplier(resp.FunName)
		bzResp := supplier()
		if err := ptypes.UnmarshalAny(resp.Body, supplier()); err != nil {
			return err
		}
		done.Resp = bzResp
		done.Run(entity.StatusOK())
		return nil
	}).DoOnCancel(func() {
		done.Run(entity.NewStatus(entity.ECanceled, "RPC request was canceled by future."))
	}).DoOnError(func(e error) {
		done.Run(entity.NewStatus(entity.Unknown, e.Error()))
	})
}
