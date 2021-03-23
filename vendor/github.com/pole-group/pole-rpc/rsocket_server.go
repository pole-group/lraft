// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"reflect"
	"sync"
	"unsafe"

	"github.com/jjeffcaii/reactor-go"
	reactorF "github.com/jjeffcaii/reactor-go/flux"
	reactorM "github.com/jjeffcaii/reactor-go/mono"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
	"google.golang.org/protobuf/proto"
)

var (
	ErrorNotImplement = errors.New("not implement")
)

type RSocketDispatcher struct {
	dispatcher
	pool scheduler.Scheduler
}

//newRSocketDispatcher 请求分发，主要作用是将每一个 ServerRequest 分发给对应的 Handler
func newRSocketDispatcher(label string) *RSocketDispatcher {
	return &RSocketDispatcher{
		dispatcher: newDispatcher(label),
		pool:       scheduler.NewElastic(32),
	}
}

//createRequestResponse
func (r *RSocketDispatcher) createRequestResponse() rsocket.OptAbstractSocket {
	return rsocket.RequestResponse(func(msg payload.Payload) mono.Mono {
		body := msg.Data()
		req := &ServerRequest{}
		err := proto.Unmarshal(body, req)
		if err != nil {
			return mono.Error(err)
		}

		if handler := r.FindReqRespHandler(req.GetFunName()); handler != nil {
			return mono.Raw(reactorM.Create(func(ctx context.Context, sink reactorM.Sink) {
				rpcCtx := newOnceRsRpcContext(sink)
				rpcCtx.req.Store(req)
				handler(context.Background(), rpcCtx)
			}).Map(func(any reactor.Any) (reactor.Any, error) {
				resp := any.(*ServerResponse)
				resp.RequestId = req.RequestId
				resp.FunName = req.FunName
				bs, err := proto.Marshal(resp)
				if err != nil {
					return nil, err
				} else {
					return payload.New(bs, EmptyBytes), nil
				}
			})).DoOnError(func(e error) {
				RpcLog.Error("an exception occurred while processing the request : %#v", e)
			})
		}
		return mono.Error(ErrorNotImplement)
	})
}

//createRequestChannel
func (r *RSocketDispatcher) createRequestChannel() rsocket.OptAbstractSocket {
	return rsocket.RequestChannel(func(requests flux.Flux) (responses flux.Flux) {
		rpcCtx := newMultiRsRpcContext()
		requests.
			DoOnNext(func(input payload.Payload) error {
				var err error
				req := &ServerRequest{}
				err = proto.Unmarshal(input.Data(), req)
				if err == nil {
					if handler := r.FindReqChannelHandler(req.GetFunName()); handler != nil {
						rpcCtx.req.Store(req)
						handler(context.Background(), rpcCtx)
					} else {
						err = ErrorNotImplement
					}
				}
				if err != nil {
					return rpcCtx.Send(&ServerResponse{
						Code: 0,
						Msg:  err.Error(),
					})
				}
				return nil
			}).
			DoOnError(func(e error) {
				RpcLog.Error("an exception occurred while processing the request : %#v", e)
				_ = rpcCtx.Send(&ServerResponse{
					Code: -1,
					Msg:  e.Error(),
				})
			}).
			Subscribe(context.Background())

		// 这里会监听 RpcContext 的 chan，然后不断发布事件，由 Server 去推送回给客户端
		return flux.Raw(reactorF.Create(func(ctx context.Context, sink reactorF.Sink) {
			for resp := range rpcCtx.fSink {
				sink.Next(resp)
			}
			sink.Complete()
		}).Map(func(any reactor.Any) (reactor.Any, error) {
			resp := any.(*ServerResponse)
			bs, err := proto.Marshal(resp)
			if err != nil {
				return nil, err
			} else {
				return payload.New(bs, EmptyBytes), nil
			}
		})).DoOnComplete(func() {
			rpcCtx.Complete()
		}).DoOnError(func(e error) {
			rpcCtx.Complete()
			RpcLog.Error("occur error : %#v", e)
		})
	})
}

type RSocketServer struct {
	IsReady    chan int8
	dispatcher *RSocketDispatcher
	ConnMgr    *ConnManager
	ErrChan    chan error
}

//AddConnectEventListener
func (rs *RSocketServer) AddConnectEventListener(listener ConnectEventListener) {
	rs.ConnMgr.AddConnectEventListener(listener)
}

//RemoveConnectEventListener
func (rs *RSocketServer) RemoveConnectEventListener(listener ConnectEventListener) {
	rs.ConnMgr.RemoveConnectEventListener(listener)
}

func (rs *RSocketServer) RegisterRequestHandler(funName string, handler RequestResponseHandler) {
	rs.dispatcher.registerRequestResponseHandler(funName, handler)
}

func (rs *RSocketServer) RegisterChannelRequestHandler(funName string, handler RequestChannelHandler) {
	rs.dispatcher.registerRequestChannelHandler(funName, handler)
}

//newRSocketServer 创建一个 RSocketServer
func newRSocketServer(ctx context.Context, opt ServerOption) *RSocketServer {
	r := RSocketServer{
		IsReady:    make(chan int8),
		dispatcher: newRSocketDispatcher(opt.Label),
		ErrChan:    make(chan error),
		ConnMgr: &ConnManager{
			rwLock: sync.RWMutex{},
			listeners: NewConcurrentSlice(func(opts *CSliceOptions) {
				opts.capacity = 16
			}),
			connRepository: map[string]net.Conn{},
		},
	}

	go func(rServer *RSocketServer) {
		server := rsocket.Receive().
			OnStart(func() {
				r.IsReady <- int8(1)
				r.ErrChan <- nil
			}).
			Acceptor(func(setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) (socket rsocket.RSocket, err error) {
				return rsocket.NewAbstractSocket(r.dispatcher.createRequestResponse(), r.dispatcher.createRequestChannel()), nil
			}).
			Transport(func(ctx context.Context) (transport.ServerTransport, error) {
				serverTransport := transport.NewTCPServerTransport(func(ctx context.Context) (net.Listener, error) {
					var listener net.Listener
					var err error
					listener, err = net.ListenTCP("tcp", &net.TCPAddr{
						IP:   net.ParseIP("0.0.0.0"),
						Port: int(opt.Port),
						Zone: "",
					})
					if err != nil {
						return nil, err
					}
					if opt.OpenTSL {
						listener = tls.NewListener(listener, &tls.Config{})
					}
					return listener, err
				})
				return &poleServerTransport{rServer: rServer, target: serverTransport}, nil
			})

		r.ErrChan <- server.Serve(ctx)
	}(&r)

	return &r
}

type ConnManager struct {
	rwLock         sync.RWMutex
	listeners      *ConcurrentSlice
	connRepository map[string]net.Conn
}

//AddConnectEventListener
func (cm *ConnManager) AddConnectEventListener(listener ConnectEventListener) {
	cm.listeners.Add(listener)
}

//RemoveConnectEventListener
func (cm *ConnManager) RemoveConnectEventListener(listener ConnectEventListener) {
	cm.listeners.Remove(listener)
}

//PutConn 添加一个 transport.TCPConn
func (cm *ConnManager) PutConn(conn *transport.TCPConn) {
	d := reflect.ValueOf(conn).Elem()
	cf := d.FieldByName("conn")
	cf = reflect.NewAt(cf.Type(), unsafe.Pointer(cf.UnsafeAddr())).Elem()
	netCon := cf.Interface().(net.Conn)

	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	cm.connRepository[netCon.RemoteAddr().String()] = netCon

	Go(netCon, func(arg interface{}) {
		conn := arg.(net.Conn)
		cm.listeners.ForEach(func(index int32, v interface{}) {
			listener := v.(ConnectEventListener)
			listener(ConnectEventForConnected, conn)
		})
	})
}

//RemoveConn 移除某一个 transport.TCPConn
func (cm *ConnManager) RemoveConn(conn *transport.TCPConn) {
	d := reflect.ValueOf(conn).Elem()
	cf := d.FieldByName("conn")
	cf = reflect.NewAt(cf.Type(), unsafe.Pointer(cf.UnsafeAddr())).Elem()
	netCon := cf.Interface().(net.Conn)

	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	delete(cm.connRepository, netCon.RemoteAddr().String())

	Go(netCon, func(arg interface{}) {
		conn := arg.(net.Conn)
		cm.listeners.ForEach(func(index int32, v interface{}) {
			listener := v.(ConnectEventListener)
			listener(ConnectEventForDisConnected, conn)
		})
	})
}

type poleServerTransport struct {
	rServer *RSocketServer
	target  transport.ServerTransport
}

// Accept register incoming connection handler.
func (p *poleServerTransport) Accept(acceptor transport.ServerTransportAcceptor) {
	proxy := func(ctx context.Context, tp *transport.Transport, onClose func(*transport.Transport)) {
		p.rServer.ConnMgr.PutConn(tp.Connection().(*transport.TCPConn))
		wrapperOnClose := func(tp *transport.Transport) {
			p.rServer.ConnMgr.RemoveConn(tp.Connection().(*transport.TCPConn))
			onClose(tp)
		}
		acceptor(ctx, tp, wrapperOnClose)
	}

	p.target.Accept(proxy)
}

// Listen listens on the network address addr and handles requests on incoming connections.
// You can specify notifier chan, it'll be sent true/false when server listening success/failed.
func (p *poleServerTransport) Listen(ctx context.Context, notifier chan<- bool) error {
	return p.target.Listen(ctx, notifier)
}

// Closer is the interface that wraps the basic Close method.
//
// The behavior of Close after the first call is undefined.
// Specific implementations may document their own behavior.
func (p *poleServerTransport) Close() error {
	return p.target.Close()
}
