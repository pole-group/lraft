package transport

import (
	"context"
	"crypto/tls"
	"net"
	"reflect"
	"sync"

	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/payload"
)

type RSocketServer struct {
	IsReady    chan struct{}
	dispatcher *Dispatcher
	ConnMgr    *ConnManager
}

func (rs *RSocketServer) RegisterRequestHandler(path string, handler ServerHandler)  {
	rs.dispatcher.registerRequestResponseHandler(path, handler)
}

func (rs *RSocketServer) RegisterStreamRequestHandler(path string, handler ServerHandler) {
	rs.dispatcher.registerRequestChannelHandler(path, handler)
}

func NewRSocketServer(ctx context.Context, label string, port int64, openTSL bool) *RSocketServer {
	r := RSocketServer{
		IsReady:    make(chan struct{}),
		dispatcher: newDispatcher(label),
	}

	go func(rServer *RSocketServer) {
		server := rsocket.Receive().
			OnStart(func() {
				close(r.IsReady)
			}).
			Acceptor(func(setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) (socket rsocket.RSocket, err error) {
				return rsocket.NewAbstractSocket(r.dispatcher.createRequestResponseSocket(), r.dispatcher.createRequestChannelSocket()), nil
			}).
			Transport(func(ctx context.Context) (transport.ServerTransport, error) {
				serverTransport := transport.NewTCPServerTransport(func(ctx context.Context) (net.Listener, error) {
					var listener net.Listener
					var err error
					listener, err = net.ListenTCP("tcp", &net.TCPAddr{
						IP:   net.ParseIP("0.0.0.0"),
						Port: int(port),
						Zone: "",
					})
					if err != nil {
						return nil, err
					}
					if openTSL {
						listener = tls.NewListener(listener, &tls.Config{})
					}
					return listener, err
				})
				return &lraftServerTransport{rServer: rServer, target: serverTransport}, nil
			})

		if err := server.Serve(ctx); err != nil {
			panic(err)
		}
	}(&r)

	return &r
}

type ConnManager struct {
	rwLock         sync.RWMutex
	connRepository map[string]net.Conn
}

func (cm *ConnManager) PutConn(conn transport.Conn) {
	d := reflect.ValueOf(conn)
	netCon := d.FieldByName("conn").Interface().(net.Conn)

	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	cm.connRepository[netCon.RemoteAddr().String()] = netCon
}

func (cm *ConnManager) RemoveConn(conn transport.Conn) {
	d := reflect.ValueOf(conn)
	netCon := d.FieldByName("conn").Interface().(net.Conn)

	defer cm.rwLock.Unlock()
	cm.rwLock.Lock()
	delete(cm.connRepository, netCon.RemoteAddr().String())
}

type lraftServerTransport struct {
	rServer *RSocketServer
	target  transport.ServerTransport
}

// Accept register incoming connection handler.
func (p *lraftServerTransport) Accept(acceptor transport.ServerTransportAcceptor) {
	proxy := func(ctx context.Context, tp *transport.Transport, onClose func(*transport.Transport)) {
		p.rServer.ConnMgr.PutConn(tp.Connection())

		wrapperOnClose := func(tp *transport.Transport) {
			p.rServer.ConnMgr.PutConn(tp.Connection())
			onClose(tp)
		}

		acceptor(ctx, tp, wrapperOnClose)
	}
	p.target.Accept(proxy)
}

// Listen listens on the network address addr and handles requests on incoming connections.
// You can specify notifier chan, it'll be sent true/false when server listening success/failed.
func (p *lraftServerTransport) Listen(ctx context.Context, notifier chan<- bool) error {
	return p.target.Listen(ctx, notifier)
}

func (p *lraftServerTransport) Close() error {
	return p.target.Close()
}
