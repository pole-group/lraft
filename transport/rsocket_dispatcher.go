package transport

import (
	"context"
	"errors"
	"sync"

	"lraft/logger"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
)

var (
	ErrorNotImplement = errors.New("not implement")
)

type Dispatcher struct {
	Label   string
	lock    sync.Mutex
	filters []func(req RSocketRequest) error
	logger  logger.Logger

	reqRespHandler map[string]struct {
		supplier func() proto.Message
		handler  func(payload payload.Payload, req proto.Message, sink mono.Sink)
	}
	reqChannelHandler map[string]struct {
		supplier func() proto.Message
		handler  func(payload payload.Payload, req proto.Message, sink flux.Sink)
	}
}

func NewDispatcher(label string) *Dispatcher {
	return &Dispatcher{
		Label: label,
		reqRespHandler: make(map[string]struct {
			supplier func() proto.Message
			handler  func(payload payload.Payload, req proto.Message, sink mono.Sink)
		}),
		reqChannelHandler: make(map[string]struct {
			supplier func() proto.Message
			handler  func(payload payload.Payload, req proto.Message, sink flux.Sink)
		}),
	}
}

type RSocketRequest struct {
	Msg payload.Payload
	Req *GrpcRequest
}

func (r *Dispatcher) SetLogger(logger logger.Logger) {
	r.logger = logger
}

func (r *Dispatcher) RegisterFilter(chain ...func(req RSocketRequest) error) {
	r.filters = append(r.filters, chain...)
}

func (r *Dispatcher) CreateRequestResponseSocket() rsocket.OptAbstractSocket {
	return rsocket.RequestResponse(func(msg payload.Payload) mono.Mono {
		body := msg.Data()
		gRPCRep := &GrpcRequest{}
		err := proto.Unmarshal(body, gRPCRep)
		if err != nil {
			return mono.Error(err)
		}

		if wrap, ok := r.reqRespHandler[gRPCRep.GetLabel()]; ok {
			req := RSocketRequest{
				Msg: msg,
				Req: gRPCRep,
			}

			for _, filter := range r.filters {
				if err := filter(req); err != nil {
					return mono.Error(err)
				}
			}

			return mono.Create(func(ctx context.Context, sink mono.Sink) {
				any := gRPCRep.GetBody()
				pb := wrap.supplier()

				err := ptypes.UnmarshalAny(any, pb)

				if err != nil {
					sink.Error(err)
				} else {
					wrap.handler(msg, pb, sink)
				}
			}).DoOnError(func(e error) {
				r.logger.Error("an exception occurred while processing the request %s", e)
			})
		}
		return mono.Error(ErrorNotImplement)
	})
}

func (r *Dispatcher) CreateRequestChannelSocket() rsocket.OptAbstractSocket {
	return rsocket.RequestChannel(func(msgs rx.Publisher) flux.Flux {
		return flux.Create(func(ctx context.Context, sink flux.Sink) {
			msgs.(flux.Flux).SubscribeOn(scheduler.Elastic()).
				DoOnNext(func(input payload.Payload) {
					body := input.Data()
					gRPCRep := &GrpcRequest{}
					err := proto.Unmarshal(body, gRPCRep)
					if err != nil {
						panic(err)
					}
					if wrap, ok := r.reqChannelHandler[gRPCRep.GetLabel()]; ok {
						req := RSocketRequest{
							Msg: input,
							Req: gRPCRep,
						}

						hasError := false
						for _, filter := range r.filters {
							e := filter(req)
							if e != nil {
								hasError = true
								sink.Error(e)
								break
							}
						}
						if !hasError {
							wrap.handler(input, gRPCRep.GetBody(), sink)
						}
					} else {
						sink.Error(ErrorNotImplement)
					}
				}).
				DoOnError(func(e error) {
					r.logger.Error("an exception occurred while processing the request %s", e)
				}).
				Subscribe(context.Background())
		})
	})
}

func (r *Dispatcher) RegisterRequestResponseHandler(key string, supplier func() proto.Message,
	handler func(input payload.Payload, req proto.Message, sink mono.Sink)) {
	defer func() {
		r.lock.Unlock()
		if err := recover(); err != nil {
			r.logger.Error("register rep&resp handler has error %s", err)
		}
	}()
	r.lock.Lock()

	if _, ok := r.reqRespHandler[key]; ok {
		return
	}
	r.reqRespHandler[key] = struct {
		supplier func() proto.Message
		handler  func(payload payload.Payload, req proto.Message, sink mono.Sink)
	}{supplier: supplier, handler: handler}
}

func (r *Dispatcher) RegisterRequestChannelHandler(key string, supplier func() proto.Message,
	handler func(input payload.Payload, req proto.Message, sink flux.Sink)) {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqChannelHandler[key]; ok {
		return
	}
	r.reqChannelHandler[key] = struct {
		supplier func() proto.Message
		handler  func(payload payload.Payload, req proto.Message, sink flux.Sink)
	}{supplier: supplier, handler: handler}
}
