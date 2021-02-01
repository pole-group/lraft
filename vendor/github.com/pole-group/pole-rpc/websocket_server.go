// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/websocket"
)

type webSocketServerRpcContext struct {
	req  atomic.Value
	once bool
	sink chan *ServerResponse
}

func newWsServerRpcContext(conn *websocket.Conn, size int32) RpcServerContext {
	rpcCtx := &webSocketServerRpcContext{
		req:  atomic.Value{},
		once: size == 1,
		sink: make(chan *ServerResponse, size),
	}

	go func(conn *websocket.Conn) {
		for resp := range rpcCtx.sink {
			req := rpcCtx.GetReq()
			resp.RequestId = req.GetRequestId()
			returnResp(resp, conn)
		}
	}(conn)

	return rpcCtx
}

func (rpcCtx *webSocketServerRpcContext) GetReq() *ServerRequest {
	return rpcCtx.req.Load().(*ServerRequest)
}

func (rpcCtx *webSocketServerRpcContext) Send(resp *ServerResponse) {
	rpcCtx.sink <- resp
	if rpcCtx.once {
		rpcCtx.Complete()
		return
	}
}

func (rpcCtx *webSocketServerRpcContext) Complete() {
	close(rpcCtx.sink)
}

//TODO 待完成
type WebSocketServer struct {
	dispatcher
	ctx     context.Context
	port    int32
	openTSL bool
}

func newWebSocketServer(ctx context.Context, label string, port int32, openTSL bool) TransportServer {
	wss := &WebSocketServer{
		dispatcher: newDispatcher(label),
		ctx:        ctx,
		port:       port,
		openTSL:    openTSL,
	}

	wss.start()
	return wss
}

func (wss *WebSocketServer) start() {
	go func() {
		http.HandleFunc("/pole_rpc", wss.serveWs)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", wss.port), nil); err != nil {
			panic(err)
		}
	}()
}

func (wss *WebSocketServer) serveWs(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		if _, ok := err.(websocket.HandshakeError); !ok {
			log.Println(err)
		}
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		returnResp(&ServerResponse{
			Code: -1,
			Msg:  err.Error(),
		}, ws)
	}
	req := new(ServerRequest)
	if err := proto.Unmarshal(body, req); err != nil {
		returnResp(&ServerResponse{
			Code: -1,
			Msg:  err.Error(),
		}, ws)
		return
	}
	if "channel" == req.Header["ws_type"] {
		handler := wss.FindReqChannelHandler(req.GetFunName())
		handler(context.Background(), newWsServerRpcContext(ws, 32))
	} else {
		handler := wss.FindReqRespHandler(req.GetFunName())
		handler(context.Background(), newWsServerRpcContext(ws, 1))
	}
}

func returnResp(resp *ServerResponse, conn *websocket.Conn) {
	result, _ := proto.Marshal(resp)
	if err := conn.WriteMessage(websocket.BinaryMessage, result); err != nil {
		RpcLog.Error("")
		return
	}
}

func (wss *WebSocketServer) RegisterRequestHandler(funName string, handler RequestResponseHandler) {
	wss.dispatcher.registerRequestResponseHandler(funName, handler)
}

func (wss *WebSocketServer) RegisterChannelRequestHandler(funName string, handler RequestChannelHandler) {
	wss.dispatcher.registerRequestChannelHandler(funName, handler)
}
