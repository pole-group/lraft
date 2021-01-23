// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"fmt"
	"sync/atomic"

	reactorF "github.com/jjeffcaii/reactor-go/flux"
	reactorM "github.com/jjeffcaii/reactor-go/mono"
)

var ErrorCannotResponse = fmt.Errorf("cann't send reponse to client")

type rSocketClientRpcContext struct {
	fSink reactorF.Sink
}

func (rpc *rSocketClientRpcContext) Send(resp *ServerRequest) {
	rpc.fSink.Next(resp)
	return
}

type rSocketServerRpcContext struct {
	req   atomic.Value
	mSink reactorM.Sink
	fSink chan *ServerResponse
}

func newOnceRsRpcContext() *rSocketServerRpcContext {
	return &rSocketServerRpcContext{
		req: atomic.Value{},
	}
}

func newMultiRsRpcContext() *rSocketServerRpcContext {
	return &rSocketServerRpcContext{
		req:   atomic.Value{},
		fSink: make(chan *ServerResponse, 32),
	}
}

func (rpc *rSocketServerRpcContext) GetReq() *ServerRequest {
	return rpc.req.Load().(*ServerRequest)
}

func (rpc *rSocketServerRpcContext) Send(resp *ServerResponse) {
	req := rpc.GetReq()
	resp.FunName = req.FunName
	resp.RequestId = req.RequestId
	if rpc.fSink != nil {
		rpc.fSink <- resp
		return
	}
	if rpc.mSink != nil {
		rpc.mSink.Success(resp)
		return
	}
	panic(ErrorCannotResponse)
}

func (rpc *rSocketServerRpcContext) Complete() {
	if rpc.mSink != nil {
		rpc.mSink = nil
	}
	if rpc.fSink != nil {
		close(rpc.fSink)
	}
}
