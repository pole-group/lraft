// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"net"
	"strconv"
)

//go:generate stringer -type ConnectEventType
type ConnectEventType int8

const (
	ConnectEventForConnected ConnectEventType = iota
	ConnectEventForDisConnected
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[ConnectEventForConnected-0]
	_ = x[ConnectEventForDisConnected-1]
}

const _ConnectEventType_name = "ConnectEventForConnectedConnectEventForDisConnected"

var _ConnectEventType_index = [...]uint8{0, 24, 51}

func (i ConnectEventType) String() string {
	if i < 0 || i >= ConnectEventType(len(_ConnectEventType_index)-1) {
		return "ConnectEventType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _ConnectEventType_name[_ConnectEventType_index[i]:_ConnectEventType_index[i+1]]
}

type TransportClient interface {
	//RegisterConnectEventWatcher 客户端监听和服务端的会话的状态
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
	//AddConnectEventListener 服务端的监听和客户端的会话的状态
	AddConnectEventListener(listener ConnectEventListener)
	//RemoveConnectEventListener
	RemoveConnectEventListener(listener ConnectEventListener)
	//RegisterRequestHandler 注册一个 Request-Response的Server端处理者，名称为name
	RegisterRequestHandler(funName string, handler RequestResponseHandler)
	//RegisterChannelRequestHandler 注册一个 Request-Channel的Server端处理者，名称为name
	RegisterChannelRequestHandler(funName string, handler RequestChannelHandler)
}

type ServerOptions func(opt *ServerOption)

type ServerOption struct {
	ConnectType ConnectType
	Label       string
	Port        int32
	OpenTSL     bool
}

type ClientOptions func(opt *ClientOption)

type ClientOption struct {
	ConnectType ConnectType
	OpenTSL     bool
}

//NewTransportServer 构建一个 TransportServer
func NewTransportServer(ctx context.Context, options ...ServerOptions) (TransportServer, error) {
	opt := new(ServerOption)

	for _, option := range options {
		option(opt)
	}

	switch opt.ConnectType {
	case ConnectTypeRSocket:
		return newRSocketServer(ctx, *opt), nil
	case ConnectWebSocket:
		return newWebSocketServer(ctx, *opt), nil
	default:
		return nil, nil
	}
}

//NewTransportClient 构建一个 TransportClient
func NewTransportClient(options ...ClientOptions) (TransportClient, error) {
	opt := new(ClientOption)

	for _, option := range options {
		option(opt)
	}

	switch opt.ConnectType {
	case ConnectTypeRSocket:
		return newRSocketClient(*opt)
	case ConnectWebSocket:
		return newWebSocketClient(*opt)
	default:
		return nil, nil
	}
}
