// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"net"
	"sync"
	"time"
)

type RpcClientContext interface {
	Send(resp *ServerRequest)
}

type RpcServerContext interface {

	GetReq() *ServerRequest

	Send(resp *ServerResponse) error

	Complete()
}

type UserCall func(resp *ServerResponse, err error)

type RequestResponseHandler func(cxt context.Context, rpcCtx RpcServerContext)

type RequestChannelHandler func(cxt context.Context, rpcCtx RpcServerContext)

const (
	RequestID = "Pole-Request-ID"
)

var EmptyBytes []byte

type proxyConn struct {
	Target  net.Conn
	OnClose func(conn net.Conn)
}

func (c *proxyConn) Read(b []byte) (n int, err error) {
	return c.Target.Read(b)
}

func (c *proxyConn) Write(b []byte) (n int, err error) {
	return c.Target.Write(b)
}

func (c *proxyConn) Close() error {
	c.OnClose(c.Target)
	return c.Target.Close()
}

func (c *proxyConn) LocalAddr() net.Addr {
	return c.Target.LocalAddr()
}

func (c *proxyConn) RemoteAddr() net.Addr {
	return c.Target.RemoteAddr()
}

func (c *proxyConn) SetDeadline(t time.Time) error {
	return c.Target.SetDeadline(t)
}

func (c *proxyConn) SetReadDeadline(t time.Time) error {
	return c.Target.SetReadDeadline(t)
}

func (c *proxyConn) SetWriteDeadline(t time.Time) error {
	return c.Target.SetWriteDeadline(t)
}

type ConnectEventListener func(eventType ConnectEventType, con net.Conn)

type ConnectEvent struct {
	EventType ConnectEventType
	Conn      net.Conn
}

type BaseTransportClient struct {
	rwLock    sync.RWMutex
	EventChan chan ConnectEvent
	Watchers  []ConnectEventListener
	Filters   []func(req *ServerRequest)
	CancelFs  []context.CancelFunc
}

func newBaseClient() *BaseTransportClient {
	bClient := &BaseTransportClient{
		rwLock:    sync.RWMutex{},
		Watchers:  make([]ConnectEventListener, 0),
		EventChan: make(chan ConnectEvent, 16),
		Filters:   make([]func(req *ServerRequest), 0),
	}
	bClient.startConnectEventListener()
	return bClient
}

func (btc *BaseTransportClient) startConnectEventListener() {
	ctx, cancelf := context.WithCancel(context.Background())
	btc.CancelFs = append(btc.CancelFs, cancelf)
	go func(ctx context.Context) {
		for {
			select {
			case element := <-btc.EventChan:
				handle := func(event ConnectEvent) {
					defer btc.rwLock.RUnlock()
					btc.rwLock.RLock()
					for _, watcher := range btc.Watchers {
						watcher(element.EventType, event.Conn)
					}
				}
				handle(element)
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
}

func (btc *BaseTransportClient) AddWatcher(watcher ConnectEventListener) {
	defer btc.rwLock.Unlock()
	btc.rwLock.Lock()
	btc.Watchers = append(btc.Watchers, watcher)
}

func (btc *BaseTransportClient) AddChain(filter func(req *ServerRequest)) {
	defer btc.rwLock.Unlock()
	btc.rwLock.Lock()
	btc.Filters = append(btc.Filters, filter)
}

func (btc *BaseTransportClient) DoFilter(req *ServerRequest) {
	btc.rwLock.RLock()
	for _, filter := range btc.Filters {
		filter(req)
	}
	btc.rwLock.RUnlock()
}

type ConnectType string

const (
	ConnectTypeRSocket ConnectType = "RSocket"
	ConnectWebSocket   ConnectType = "WebSocket"
)
