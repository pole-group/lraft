package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go"
	flux2 "github.com/jjeffcaii/reactor-go/flux"
	mono2 "github.com/jjeffcaii/reactor-go/mono"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core/transport"
	rtp "github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"

	"github.com/pole-group/lraft/utils"
)

type ConnectEventType int8

const (
	ConnectEventForConnected ConnectEventType = iota
	ConnectEventForDisConnected
)

type ConnectEventWatcher func(eventType ConnectEventType, con net.Conn)

type connectEvent struct {
	eventType ConnectEventType
	conn      net.Conn
}

type RSocketClient struct {
	socket    rsocket.Client
	bc        *baseTransportClient
	rwLock    sync.RWMutex
	watchers  *utils.Set
	eventChan chan connectEvent
	cancelfs  []context.CancelFunc
}

func newRSocketClient(serverAddr string, openTSL bool) (*RSocketClient, error) {

	client := &RSocketClient{
		bc:        newBaseClient(),
		rwLock:    sync.RWMutex{},
		watchers:  utils.NewSet(),
		eventChan: make(chan connectEvent, 16),
	}

	ip, port := utils.AnalyzeIPAndPort(serverAddr)
	c, err := rsocket.Connect().
		OnClose(func(err error) {

		}).
		OnConnect(func(client rsocket.Client, err error) {

		}).
		Transport(func(ctx context.Context) (*transport.Transport, error) {
			var conn net.Conn
			var dial net.Dialer
			conn, err := dial.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", ip, port))
			if err != nil {
				return nil, err
			}
			if openTSL {
				conn = tls.Client(conn, &tls.Config{})
			}

			conn = &wrapperConn{
				target: conn,
				onClose: func(conn net.Conn) {
					client.eventChan <- connectEvent{
						eventType: ConnectEventForDisConnected,
						conn:      conn,
					}
				},
			}

			client.eventChan <- connectEvent{
				eventType: ConnectEventForConnected,
				conn:      conn,
			}

			trp := rtp.NewTCPClientTransport(conn)
			return trp, err
		}).
		Start(context.Background())

	client.socket = c
	return client, err
}

func (c *RSocketClient) startConnectEventListener() {
	ctx, cancelf := context.WithCancel(context.Background())
	c.cancelfs = append(c.cancelfs, cancelf)
	utils.NewGoroutine(ctx, func(ctx context.Context) {
		for {
			select {
			case element := <-c.eventChan:
				handle := func(event connectEvent) {
					defer c.rwLock.RUnlock()
					c.rwLock.RLock()
					c.watchers.Range(func(value interface{}) {
						value.(ConnectEventWatcher)(element.eventType, event.conn)
					})
				}
				handle(element)
			case <-ctx.Done():

			}
		}
	})
}

func (c *RSocketClient) RegisterConnectEventWatcher(watcher func(eventType ConnectEventType, conn net.Conn)) {
	defer c.rwLock.Unlock()
	c.rwLock.Lock()
	c.watchers.Add(watcher)
}

func (c *RSocketClient) AddChain(filter func(req *GrpcRequest)) {
	c.bc.AddChain(filter)
}

func (c *RSocketClient) Request(req *GrpcRequest) (mono2.Mono, error) {
	c.bc.DoFilter(req)

	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	return c.socket.RequestResponse(payload.New(body, utils.EmptyBytes)).
		Raw().
		FlatMap(func(any mono2.Any) mono2.
		Mono {
			payLoad := any.(payload.Payload)
			resp := new(GrpcResponse)
			if err := proto.Unmarshal(payLoad.Data(), resp); err != nil {
				return mono2.Error(err)
			}
			return mono2.Just(resp)
		}), nil
}

func (c *RSocketClient) RequestChannel(call func(resp *GrpcResponse, err error)) flux2.Sink {
	var _sink flux2.Sink
	f := flux2.Create(func(ctx context.Context, sink flux2.Sink) {
		_sink = sink
	}).Map(func(any reactor.Any) (reactor.Any, error) {
		req := any.(*GrpcRequest)
		c.bc.DoFilter(req)
		body, err := proto.Marshal(req)
		if err != nil {
			return nil, err
		}
		return payload.New(body, utils.EmptyBytes), nil
	})
	c.socket.RequestChannel(flux.Raw(f)).Raw().DoOnNext(func(v reactor.Any) error {
		payLoad := v.(payload.Payload)
		resp := new(GrpcResponse)
		if err := proto.Unmarshal(payLoad.Data(), resp); err != nil {
			return err
		}
		call(resp, nil)
		return nil
	}).DoOnError(func(e error) {
		call(nil, e)
	})
	return _sink
}

func (c *RSocketClient) Close() error {
	for _, cancelF := range c.cancelfs {
		cancelF()
	}
	return nil
}
