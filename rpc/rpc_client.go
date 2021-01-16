package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/jjeffcaii/reactor-go/mono"

	"github.com/pole-group/lraft/entity"
	"github.com/pole-group/lraft/transport"
)

type RaftClient struct {
	lock     sync.RWMutex
	sockets  map[string]rpcClient
	openTSL  bool
	watchers []func(con *net.Conn)
}

type rpcClient struct {
	client transport.TransportClient
	ctx    context.Context
}

func NewRaftClient(openTSL bool) *RaftClient {
	return &RaftClient{
		sockets: make(map[string]rpcClient),
		openTSL: openTSL,
	}
}

func (c *RaftClient) RegisterConnectEventWatcher(watcher func(event transport.ConnectEventType, con net.Conn)) {
	for _, socket := range c.sockets {
		socket.client.RegisterConnectEventWatcher(watcher)
	}
}

func (c *RaftClient) computeIfAbsent(endpoint entity.Endpoint) {
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

func (c *RaftClient) SendRequest(endpoint entity.Endpoint, req *transport.GrpcRequest) (mono.Mono, error) {
	c.computeIfAbsent(endpoint)
	if rpcC, exist := c.sockets[endpoint.GetDesc()]; exist {
		return rpcC.client.Request(req)
	}
	return nil, ServerNotFount
}
