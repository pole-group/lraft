package rpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/jjeffcaii/reactor-go/mono"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/transport"
)

type RaftRPCClient struct {
	lock    sync.RWMutex
	sockets map[string]rpcClient
	openTSL bool
}

type rpcClient struct {
	client transport.TransportClient
	ctx    context.Context
}

func NewRaftRPCClient(openTSL bool) *RaftRPCClient {
	return &RaftRPCClient{
		sockets: make(map[string]rpcClient),
		openTSL: openTSL,
	}
}

func (c *RaftRPCClient) computeIfAbsent(endpoint entity.Endpoint) {
	defer c.lock.Unlock()
	c.lock.Lock()

	if _, exist := c.sockets[endpoint.GetDesc()]; !exist {
		client, err := transport.NewTransportClient(transport.ConnectTypeRSocket, fmt.Sprintf("%s:%d",
			endpoint.GetIP(),
			endpoint.GetPort()), c.openTSL)

		if err != nil {
			panic(err)
		}
		c.sockets[endpoint.GetDesc()] = rpcClient{
			client: client,
			ctx:    context.Background(),
		}
	}
}

func (c *RaftRPCClient) SendRequest(endpoint entity.Endpoint, req *transport.GrpcRequest) (mono.Mono, error) {
	c.computeIfAbsent(endpoint)
	if rpcC, exist := c.sockets[endpoint.GetDesc()]; exist {
		return rpcC.client.Request(req)
	}
	return nil, ServerNotFount
}
