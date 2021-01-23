// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go"
	reactorF "github.com/jjeffcaii/reactor-go/flux"
	reactorM "github.com/jjeffcaii/reactor-go/mono"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
)

type RSocketClient struct {
	supplier   func(endpoint Endpoint) (rsocket.Client, error)
	repository EndpointRepository
	rwLock     sync.RWMutex
	sockets    map[string]rsocket.Client
	bc         *BaseTransportClient
}

//TODO 后续改造成为 Option 的模式
func newRSocketClient(openTSL bool, repository EndpointRepository) (*RSocketClient, error) {

	client := &RSocketClient{
		rwLock:     sync.RWMutex{},
		sockets:    make(map[string]rsocket.Client),
		bc:         newBaseClient(),
		repository: repository,
	}

	supplier := func(endpoint Endpoint) (rsocket.Client, error) {
		c, err := rsocket.Connect().
			OnClose(func(err error) {
				defer client.rwLock.Unlock()
				client.rwLock.Lock()
				delete(client.sockets, endpoint.GetKey())
			}).
			Transport(func(ctx context.Context) (*transport.Transport, error) {
				var conn net.Conn
				var dial net.Dialer
				conn, err := dial.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port))
				if err != nil {
					return nil, err
				}
				if openTSL {
					conn = tls.Client(conn, &tls.Config{})
				}

				conn = &proxyConn{
					Target: conn,
					OnClose: func(conn net.Conn) {
						client.bc.EventChan <- ConnectEvent{
							EventType: ConnectEventForDisConnected,
							Conn:      conn,
						}
					},
				}

				client.bc.EventChan <- ConnectEvent{
					EventType: ConnectEventForConnected,
					Conn:      conn,
				}

				trp := transport.NewTCPClientTransport(conn)
				return trp, err
			}).
			Start(context.Background())
		return c, err
	}

	client.supplier = supplier
	return client, nil
}

func (c *RSocketClient) RegisterConnectEventWatcher(watcher func(eventType ConnectEventType, conn net.Conn)) {
	c.bc.AddWatcher(watcher)
}

func (c *RSocketClient) AddChain(filter func(req *ServerRequest)) {
	c.bc.AddChain(filter)
}

func (c *RSocketClient) Request(ctx context.Context, name string, req *ServerRequest) (*ServerResponse, error) {
	c.bc.DoFilter(req)
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	client, err := c.computeIfAbsent(name)
	if err != nil {
		return nil, err
	}

	resp, err := client.RequestResponse(payload.New(body, EmptyBytes)).
		Raw().
		FlatMap(func(any reactorM.Any) reactorM.
		Mono {
			payLoad := any.(payload.Payload)
			resp := new(ServerResponse)
			if err := proto.Unmarshal(payLoad.Data(), resp); err != nil {
				return reactorM.Error(err)
			}
			return reactorM.Just(resp)
		}).Block(ctx)
	if err != nil {
		return nil, err
	}
	return resp.(*ServerResponse), nil
}

func (c *RSocketClient) RequestChannel(ctx context.Context, name string, call UserCall) (RpcClientContext, error) {
	client, err := c.computeIfAbsent(name)
	if err != nil {
		return nil, err
	}

	rpcCtx := &rSocketClientRpcContext{}

	f := reactorF.Create(func(ctx context.Context, s reactorF.Sink) {
		rpcCtx.fSink = s
	}).Map(func(any reactor.Any) (reactor.Any, error) {
		req := any.(*ServerRequest)
		c.bc.DoFilter(req)
		body, err := proto.Marshal(req)
		if err != nil {
			return nil, err
		}
		return payload.New(body, EmptyBytes), nil
	})

	client.RequestChannel(flux.Raw(f)).DoOnNext(func(output payload.Payload) error {
		resp := new(ServerResponse)
		if err := proto.Unmarshal(output.Data(), resp); err != nil {
			call(nil, err)
		} else {
			call(resp, nil)
		}
		return nil
	}).DoOnError(func(e error) {
		panic(err)
	}).Subscribe(ctx)

	// 为了确保 rpcCtx 中的 fSink 被正确赋值
	// 这里是不是可以考虑直接使用 chan 做操作？
	for {
		if rpcCtx.fSink != nil {
			break
		}
	}

	return rpcCtx, err
}

func (c *RSocketClient) Close() error {
	for _, socket := range c.sockets {
		_ = socket.Close()
	}
	return nil
}

func (c *RSocketClient) computeIfAbsent(name string) (rsocket.Client, error) {
	success, endpoint := c.repository.SelectOne(name)
	if !success {
		return nil, fmt.Errorf("can't find target endpoint by service-name : %s", name)
	}

	var rClient rsocket.Client
	c.rwLock.RLock()
	if v, exist := c.sockets[endpoint.GetKey()]; exist {
		rClient = v
		c.rwLock.RUnlock()
	} else {
		c.rwLock.RUnlock()
		defer c.rwLock.Unlock()
		c.rwLock.Lock()
		client, err := c.supplier(endpoint)
		if err != nil {
			return nil, err
		}
		c.sockets[endpoint.GetKey()] = client
		rClient = client
	}
	return rClient, nil
}
