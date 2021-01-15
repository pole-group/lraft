package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
)

var (
	ErrorNotImplement = errors.New("not implement")
)

type reqRespHandler struct {
	handler ServerHandler
}

type reqChannelHandler struct {
	supplier func() proto.Message
	handler  ServerHandler
}

type Dispatcher struct {
	Label             string
	lock              sync.Mutex
	reqRespHandler    map[string]reqRespHandler
	reqChannelHandler map[string]reqChannelHandler
}

func newDispatcher(label string) *Dispatcher {
	return &Dispatcher{
		Label:             label,
		reqRespHandler:    make(map[string]reqRespHandler),
		reqChannelHandler: make(map[string]reqChannelHandler),
	}
}

func (r *Dispatcher) createRequestResponseSocket() rsocket.OptAbstractSocket {
	return rsocket.RequestResponse(func(msg payload.Payload) mono.Mono {
		body := msg.Data()
		req := &GrpcRequest{}
		err := proto.Unmarshal(body, req)
		if err != nil {
			return mono.Error(err)
		}

		if wrap, ok := r.reqRespHandler[req.GetLabel()]; ok {
			return mono.Create(func(ctx context.Context, sink mono.Sink) {
				result := wrap.handler(context.Background(), req)
				bs, err := proto.Marshal(result)
				if err != nil {
					sink.Error(err)
				} else {
					sink.Success(payload.New(bs, []byte{}))
				}
			}).DoOnError(func(e error) {
				fmt.Printf("an exception occurred while processing the request %s\n", err)
			})
		}
		return mono.Error(ErrorNotImplement)
	})
}

func (r *Dispatcher) createRequestChannelSocket() rsocket.OptAbstractSocket {
	return rsocket.RequestChannel(func(requests flux.Flux) (responses flux.Flux) {
		return flux.Create(func(ctx context.Context, sink flux.Sink) {
			requests.SubscribeOn(scheduler.Elastic()).
				DoOnNext(func(input payload.Payload) error {
					req := &GrpcRequest{}
					err := proto.Unmarshal(input.Data(), req)
					if err != nil {
						sink.Error(err)
					}
					if wrap, ok := r.reqChannelHandler[req.GetLabel()]; ok {
						resp := wrap.handler(context.Background(), req)
						bs, err := proto.Marshal(resp)
						if err != nil {
							sink.Error(err)
						} else {
							sink.Next(payload.New(bs, []byte{}))
						}
						return nil
					} else {
						return ErrorNotImplement
					}
				}).
				DoOnError(func(e error) {
					fmt.Printf("an exception occurred while processing the request %s\n", e)
				}).
				Subscribe(ctx)
		})
	})
}

func (r *Dispatcher) registerRequestResponseHandler(key string, handler ServerHandler) {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqRespHandler[key]; ok {
		return
	}
	r.reqRespHandler[key] = reqRespHandler{
		handler: handler,
	}
}

func (r *Dispatcher) registerRequestChannelHandler(key string, handler ServerHandler) {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqChannelHandler[key]; ok {
		return
	}
	r.reqChannelHandler[key] = reqChannelHandler{
		handler: handler,
	}
}
