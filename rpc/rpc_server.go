package rpc

import (
	"context"
	"crypto/tls"
	"strconv"

	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/payload"

	"github.com/pole-group/lraft/transport"
	"github.com/pole-group/lraft/utils"
)

type RaftRPCServer struct {
	IsReady    chan struct{}
	Dispatcher *transport.dispatcher
	Ctx        context.Context
}

func NewRaftRPCServer(label string, port int64, openTSL bool, chain ...func(req transport.RSocketRequest) error) *RaftRPCServer {

	r := RaftRPCServer{
		IsReady:    make(chan struct{}),
		Dispatcher: transport.NewDispatcher(label),
		Ctx:        context.Background(),
	}

	r.Dispatcher.RegisterFilter(chain...)

	utils.NewGoroutine(r.Ctx, func(ctx context.Context) {
		start := rsocket.Receive().
			OnStart(func() {
				close(r.IsReady)
			}).
			Acceptor(func(setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) (socket rsocket.RSocket, err error) {
				return rsocket.NewAbstractSocket(r.Dispatcher.CreateRequestResponseSocket(), r.Dispatcher.CreateRequestChannelSocket()), nil
			}).
			Transport("tcp://0.0.0.0:" + strconv.FormatInt(port, 10))
		var err error
		if openTSL {
			err = start.ServeTLS(r.Ctx, &tls.Config{})
		} else {
			err = start.Serve(r.Ctx)
		}
		if err != nil {
			panic(err)
		}
	})

	return &r
}
