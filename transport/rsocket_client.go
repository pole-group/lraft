package transport

import (
	"context"
	"crypto/tls"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go"
	flux2 "github.com/jjeffcaii/reactor-go/flux"
	mono2 "github.com/jjeffcaii/reactor-go/mono"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"

	"github.com/pole-group/lraft/utils"
)
type RSocketClient struct {
	socket  rsocket.Client
	bc	*baseTransportClient
}

func newRSocketClient(serverAddr string, openTSL bool) (*RSocketClient, error) {

	client := &RSocketClient{
		bc: newBaseClient(),
	}

	ip, port := utils.AnalyzeIPAndPort(serverAddr)
	c, err := rsocket.Connect().
		OnClose(func(err error) {

		}).
		Transport(func(ctx context.Context) (*transport.Transport, error) {
			tcb := rsocket.TCPClient().SetHostAndPort(ip, int(port))
			if openTSL {
				tcb.SetTLSConfig(&tls.Config{})
			}
			return tcb.Build()(ctx)
		}).
		Start(context.Background())

	client.socket = c
	return client, err
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
	return nil
}
