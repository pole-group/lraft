// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/mono"
	"github.com/jjeffcaii/reactor-go/scheduler"
)

type webSocketRpcContext struct {
	call  UserCall
	owner *WebSocketClient
	fSink flux.Sink
}

func newWsMultiRpcContext(sink flux.Sink) RpcClientContext {
	return &webSocketRpcContext{
		fSink: sink,
	}
}

func (rpc *webSocketRpcContext) Send(req *ServerRequest) {
	defer rpc.owner.lock.Unlock()
	rpc.owner.lock.Lock()
	rpc.owner.channelFuture[req.RequestId] = rpc.call
	rpc.fSink.Next(req)
}

type WebSocketClient struct {
	lock          sync.RWMutex
	clientMap     map[string]*websocket.Conn
	supplier      func(endpoint Endpoint) (*websocket.Conn, error)
	bc            *BaseTransportClient
	future        map[string]mono.Sink
	channelFuture map[string]func(resp *ServerResponse, err error)
	done          chan bool
}

func newWebSocketClient(openTSL bool) (*WebSocketClient, error) {
	wsc := &WebSocketClient{
		lock:          sync.RWMutex{},
		clientMap:     make(map[string]*websocket.Conn),
		bc:            newBaseClient(),
		future:        make(map[string]mono.Sink),
		channelFuture: make(map[string]func(resp *ServerResponse, err error)),
		done:          make(chan bool),
	}

	supplier := func(endpoint Endpoint) (*websocket.Conn, error) {
		host := fmt.Sprintf("%s:%d", endpoint.Host, endpoint.Port)
		var u url.URL
		if openTSL {
			u = url.URL{Scheme: "wss", Host: host, Path: "/pole_rpc"}
		} else {
			u = url.URL{Scheme: "ws", Host: host, Path: "/pole_rpc"}
		}
		c, resp, err := websocket.DefaultDialer.Dial(u.String(), nil)
		if err == nil {
			wsc.bc.EventChan <- ConnectEvent{
				EventType: ConnectEventForConnected,
				Conn:      c.UnderlyingConn(),
			}
		}
		RpcLog.Info("receive response when connect ws-server : %#v", resp)
		return c, err
	}
	wsc.supplier = supplier

	wsc.AddChain(func(req *ServerRequest) {
		req.Header[RequestID] = uuid.New().String()
	})

	return wsc, nil
}

func (wsc *WebSocketClient) initServerRead(client *websocket.Conn) {
	go func(client *websocket.Conn) {
		for {
			select {
			case <-wsc.done:
				return
			default:
				_, message, err := client.ReadMessage()
				if err != nil {
					return
				}
				resp := &ServerResponse{}
				if err := proto.Unmarshal(message, resp); err != nil {
					continue
				}
				requestId := resp.Header[RequestID]
				if v, exist := wsc.future[requestId]; exist {
					v.Success(resp)
					continue
				}
				if f, exist := wsc.channelFuture[requestId]; exist {
					f(resp, nil)
					continue
				}
				RpcLog.Error("can't find target future to dispatcher response : %s", requestId)
			}
		}
	}(client)
}

func (wsc *WebSocketClient) RegisterConnectEventWatcher(watcher func(eventType ConnectEventType, conn net.Conn)) {
	wsc.bc.AddWatcher(watcher)
}

func (wsc *WebSocketClient) AddChain(filter func(req *ServerRequest)) {
	wsc.bc.AddChain(filter)
}

func (wsc *WebSocketClient) CheckConnection(endpoint Endpoint) (bool, error)  {
	conn, err := wsc.computeIfAbsent(endpoint)
	if err != nil {
		return false, err
	}
	return conn.RemoteAddr() != nil, nil
}

func (wsc *WebSocketClient) Request(ctx context.Context, endpoint Endpoint, req *ServerRequest) (*ServerResponse, error) {
	wsc.bc.DoFilter(req)

	conn, err := wsc.computeIfAbsent(endpoint)
	if err != nil {
		return nil, err
	}

	resp, err := mono.Create(func(ctx context.Context, s mono.Sink) {
		body, err := proto.Marshal(req)
		if err != nil {
			s.Error(err)
		}
		wsc.future[req.Header[RequestID]] = s
		err = conn.WriteMessage(websocket.BinaryMessage, body)
		if err != nil {
			s.Error(err)
		}

	}).Timeout(time.Duration(10) * time.Second).Block(ctx)

	if err != nil {
		return nil, err
	}

	return resp.(*ServerResponse), nil
}

func (wsc *WebSocketClient) RequestChannel(ctx context.Context, endpoint Endpoint, call UserCall) (RpcClientContext, error) {

	conn, err := wsc.computeIfAbsent(endpoint)
	if err != nil {
		return nil, err
	}

	rpcCtx := &webSocketRpcContext{
		call:  call,
		owner: wsc,
	}

	flux.
		Create(func(ctx context.Context, s flux.Sink) {
			rpcCtx.fSink = s
		}).
		Map(func(any reactor.Any) (reactor.Any, error) {
			req := any.(*ServerRequest)
			wsc.bc.DoFilter(req)
			body, err := proto.Marshal(req)
			if err != nil {
				call(nil, err)
			} else {
				err = conn.WriteMessage(websocket.BinaryMessage, body)
				if err != nil {
					call(nil, err)
				}
			}
			return nil, nil
		}).
		SubscribeOn(scheduler.NewElastic(1)).
		Subscribe(ctx)
	return rpcCtx, nil
}

func (wsc *WebSocketClient) Close() error {
	close(wsc.done)
	for _, c := range wsc.clientMap {
		_ = c.Close()
	}
	return nil
}

func (wsc *WebSocketClient) computeIfAbsent(endpoint Endpoint) (*websocket.Conn, error) {
	var wClient *websocket.Conn
	wsc.lock.RLock()
	if v, exist := wsc.clientMap[endpoint.GetKey()]; exist {
		wClient = v
		wsc.lock.RUnlock()
	} else {
		wsc.lock.RUnlock()
		defer wsc.lock.Unlock()
		wsc.lock.Lock()
		client, err := wsc.supplier(endpoint)
		if err != nil {
			return nil, err
		}
		wsc.clientMap[endpoint.GetKey()] = client
		wClient = client
		wsc.initServerRead(wClient)
	}
	return wClient, nil
}
