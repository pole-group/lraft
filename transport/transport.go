package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/mono"
)

type wrapperConn struct {
	target net.Conn
	onClose	func(conn net.Conn)
}

func (c *wrapperConn) Read(b []byte) (n int, err error) {
	return c.target.Read(b)
}

func (c *wrapperConn) Write(b []byte) (n int, err error) {
	return c.target.Write(b)
}

func (c *wrapperConn) Close() error {
	c.onClose(c.target)
	return c.target.Close()
}

func (c *wrapperConn) LocalAddr() net.Addr {
	return c.target.LocalAddr()
}

func (c *wrapperConn) RemoteAddr() net.Addr {
	return c.target.RemoteAddr()
}

func (c *wrapperConn) SetDeadline(t time.Time) error {
	return c.target.SetDeadline(t)
}

func (c *wrapperConn) SetReadDeadline(t time.Time) error {
	return c.target.SetReadDeadline(t)
}

func (c *wrapperConn) SetWriteDeadline(t time.Time) error {
	return c.target.SetWriteDeadline(t)
}

type ServerHandler func(cxt context.Context, req *GrpcRequest) *GrpcResponse

type TransportClient interface {
	RegisterConnectEventWatcher(watcher func(eventType ConnectEventType, conn net.Conn))

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


