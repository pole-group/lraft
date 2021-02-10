// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rpc

import (
	"context"
	"net"
	"sync"

	polerpc "github.com/pole-group/pole-rpc"

	"github.com/pole-group/lraft/entity"
)

//RaftClient raft rpc 客户端，负责进行远程方法调用
type RaftClient struct {
	lock     sync.RWMutex
	client   polerpc.TransportClient
	ctx      context.Context
	openTSL  bool
	watchers []func(con *net.Conn)
}

type rpcClient struct {
	client polerpc.TransportClient
	ctx    context.Context
}

//NewRaftClient 创建一个 Raft 的 RPC 客户端
func NewRaftClient(openTSL bool) (*RaftClient, error) {
	client, err := polerpc.NewTransportClient(polerpc.ConnectTypeRSocket, openTSL)
	if err != nil {
		return nil, err
	}

	return &RaftClient{
		client:  client,
		openTSL: openTSL,
	}, nil
}

//RegisterConnectEventWatcher 注册链接事件监听器
func (c *RaftClient) RegisterConnectEventWatcher(watcher func(event polerpc.ConnectEventType, con net.Conn)) {
	c.client.RegisterConnectEventWatcher(watcher)
}

//SendRequest 发送一个 RPC 请求，基于 request-response 模型
func (c *RaftClient) SendRequest(endpoint entity.Endpoint, req *polerpc.ServerRequest) (*polerpc.ServerResponse, error) {
	return c.client.Request(context.Background(), polerpc.Endpoint{
		Key:  "",
		Host: endpoint.GetIP(),
		Port: int32(endpoint.GetPort()),
	}, req)
}

//CheckConnection 检查链接是否存在，如果不存在则会自动去创建一个链接
func (c *RaftClient) CheckConnection(endpoint entity.Endpoint) (bool, error) {
	return c.client.CheckConnection(polerpc.Endpoint{
		Key:  "",
		Host: endpoint.GetIP(),
		Port: int32(endpoint.GetPort()),
	})
}
