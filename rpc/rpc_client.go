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

const ServiceName = "LRaft"

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

func NewRaftClient(openTSL bool, repository polerpc.EndpointRepository) (*RaftClient, error) {
	client, err := polerpc.NewTransportClient(polerpc.ConnectTypeRSocket, repository, openTSL)
	if err != nil {
		return nil, err
	}

	return &RaftClient{
		client:  client,
		openTSL: openTSL,
	}, nil
}

func (c *RaftClient) RegisterConnectEventWatcher(watcher func(event polerpc.ConnectEventType, con net.Conn)) {
	c.client.RegisterConnectEventWatcher(watcher)
}

func (c *RaftClient) SendRequest(endpoint entity.Endpoint, req *polerpc.ServerRequest) (*polerpc.ServerResponse, error) {
	return c.client.Request(context.Background(), endpoint.GetDesc(), req)
}

func (c *RaftClient) CheckConnection(endpoint entity.Endpoint) bool {
	return true
}
