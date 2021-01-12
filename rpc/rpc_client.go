package rpc

import (
	"context"
	"crypto/tls"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/mono"

	"lraft/entity"
	"lraft/transport"
)

type RaftRPCClient struct {
	lock       sync.RWMutex
	sockets    map[string]rpcClient
	dispatcher *transport.Dispatcher
	openTSL    bool
}

type rpcClient struct {
	client rsocket.Client
	ctx    context.Context
}

func NewRaftRPCClient(openTSL bool) *RaftRPCClient {
	return &RaftRPCClient{
		sockets:    make(map[string]rpcClient),
		dispatcher: transport.NewDispatcher("lRaft"),
		openTSL:    openTSL,
	}
}

func (c *RaftRPCClient) computeIfAbsent(endpoint entity.Endpoint) {
	defer c.lock.Unlock()
	c.lock.Lock()

	if _, exist := c.sockets[endpoint.GetDesc()]; !exist {
		start := rsocket.Connect().
			OnClose(func(err error) {

			}).
			Acceptor(func(socket rsocket.RSocket) rsocket.RSocket {
				return rsocket.NewAbstractSocket(c.dispatcher.CreateRequestResponseSocket(), c.dispatcher.CreateRequestChannelSocket())
			}).
			Transport("tcp://" + endpoint.GetDesc())
		var err error
		var client rsocket.Client

		if c.openTSL {
			client, err = start.StartTLS(context.Background(), &tls.Config{})
		} else {
			client, err = start.Start(context.Background())
		}

		if err != nil {
			panic(err)
		}
		c.sockets[endpoint.GetDesc()] = rpcClient{
			client: client,
			ctx:    context.Background(),
		}
	}
}

func (c *RaftRPCClient) SendRequest(endpoint entity.Endpoint, req *transport.GrpcRequest) mono.Mono {
	c.computeIfAbsent(endpoint)
	if rpcC, exist := c.sockets[endpoint.GetDesc()]; exist {
		body, err := proto.Marshal(req)
		if err != nil {
			return mono.Error(err)
		}
		return rpcC.client.RequestResponse(payload.New(body, EmptyBytes))
	}
	return mono.Error(ServerNotFount)
}
