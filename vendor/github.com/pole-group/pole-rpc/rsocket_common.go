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
}

type rSocketServerRpcContext struct {
	req   atomic.Value
	mSink reactorM.Sink
	fSink chan *ServerResponse
}

//newMultiRsRpcContext 创建用于 Request-Response 的 RpcServerContext
func newOnceRsRpcContext(mSink reactorM.Sink) *rSocketServerRpcContext {
	return &rSocketServerRpcContext{
		req:   atomic.Value{},
		mSink: mSink,
	}
}

//newMultiRsRpcContext 创建用于 Request-Channel 的 RpcServerContext
func newMultiRsRpcContext() *rSocketServerRpcContext {
	return &rSocketServerRpcContext{
		req:   atomic.Value{},
		fSink: make(chan *ServerResponse, 8),
	}
}

func (rpc *rSocketServerRpcContext) GetReq() *ServerRequest {
	return rpc.req.Load().(*ServerRequest)
}

func (rpc *rSocketServerRpcContext) Send(resp *ServerResponse) error {
	req := rpc.GetReq()
	resp.FunName = req.FunName
	resp.RequestId = req.RequestId
	if rpc.fSink != nil {
		rpc.fSink <- resp
		return nil
	}
	if rpc.mSink != nil {
		rpc.mSink.Success(resp)
		rpc.Complete()
		return nil
	}
	return ErrorCannotResponse
}

func (rpc *rSocketServerRpcContext) Complete() {
	if rpc.mSink != nil {
		rpc.mSink = nil
	}
	if rpc.fSink != nil {
		close(rpc.fSink)
	}
}
