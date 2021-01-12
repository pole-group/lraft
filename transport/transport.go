package transport

import (
	"context"
	"sync"

	"github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/mono"
)

type ServerHandler func(cxt context.Context, req *GrpcRequest) *GrpcResponse

type TransportClient interface {
	AddChain(filter func(req *GrpcRequest))

	Request(req *GrpcRequest) (mono.Mono, error)

	RequestChannel(call func(resp *GrpcResponse, err error)) flux.Sink

	Close() error
}

type TransportServer interface {
	RegisterRequestHandler(path string, handler ServerHandler)

	RegisterStreamRequestHandler(path string, handler ServerHandler)
}

type baseTransportClient struct {
	rwLock     sync.RWMutex
	filters    []func(req *GrpcRequest)
}

func newBaseClient() *baseTransportClient {
	return &baseTransportClient{
		rwLock:  sync.RWMutex{},
		filters: make([]func(req *GrpcRequest), 0, 0),
	}
}

func (btc *baseTransportClient) AddChain(filter func(req *GrpcRequest))  {
	defer btc.rwLock.Unlock()
	btc.rwLock.Lock()
	btc.filters = append(btc.filters, filter)
}

func (btc *baseTransportClient) DoFilter(req *GrpcRequest)  {
	btc.rwLock.RLock()
	for _, filter := range btc.filters {
		filter(req)
	}
	btc.rwLock.RUnlock()
}

type ConnectType string

const (
	ConnectTypeRSocket ConnectType = "RSocket"
	ConnectTypeHttp ConnectType = "Http"
	ConnectWebSocket ConnectType = "WebSocket"
)

func NewTransportClient(t ConnectType, serverAddr string, openTSL bool) (TransportClient, error) {
	switch t {
	case ConnectTypeRSocket:
		return newRSocketClient(serverAddr, openTSL)
	default:
		return nil, nil
	}
}


