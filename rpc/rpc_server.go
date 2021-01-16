package rpc

import (
	"context"

	"github.com/pole-group/lraft/transport"
)

type RaftRPCServer struct {
	IsReady chan struct{}
	server  transport.TransportServer
	Ctx     context.Context
	cancelF context.CancelFunc
}

func NewRaftRPCServer(label string, port int64, openTSL bool) *RaftRPCServer {
	ctx, cancelF := context.WithCancel(context.Background())

	r := RaftRPCServer{
		IsReady: make(chan struct{}),
		Ctx:     ctx,
		cancelF: cancelF,
	}

	r.server = transport.NewRSocketServer(r.Ctx, label, port, openTSL)

	return &r
}
