// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"net"
)

type TransportClient interface {
	//RegisterConnectEventWatcher 监听会话的状态
	RegisterConnectEventWatcher(watcher func(eventType ConnectEventType, conn net.Conn))
	//CheckConnection 检查链接
	CheckConnection(endpoint Endpoint) (bool, error)
	//AddChain 添加请求处理链
	AddChain(filter func(req *ServerRequest))
	//Request 发起 request-response 请求
	Request(ctx context.Context, endpoint Endpoint, req *ServerRequest) (*ServerResponse, error)
	//RequestChannel 发起 request-channel 请求
	RequestChannel(ctx context.Context, endpoint Endpoint, call UserCall) (RpcClientContext, error)
	//Close 关闭客户端
	Close() error
}

type TransportServer interface {
	RegisterRequestHandler(funName string, handler RequestResponseHandler)

	RegisterChannelRequestHandler(funName string, handler RequestChannelHandler)
}

func NewTransportServer(ctx context.Context, t ConnectType, label string, port int32, openTSL bool) (TransportServer, error) {
	switch t {
	case ConnectTypeRSocket:
		return NewRSocketServer(ctx, label, port, openTSL), nil
	case ConnectWebSocket:
		return newWebSocketServer(ctx, label, port, openTSL), nil
	default:
		return nil, nil
	}
}

func NewTransportClient(t ConnectType, openTSL bool) (TransportClient, error) {
	switch t {
	case ConnectTypeRSocket:
		return newRSocketClient(openTSL)
	case ConnectWebSocket:
		return newWebSocketClient(openTSL)
	default:
		return nil, nil
	}
}
