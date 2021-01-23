// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"sync"

	"github.com/golang/protobuf/proto"
)

type reqRespHandler struct {
	handler RequestResponseHandler
}

type reqChannelHandler struct {
	supplier func() proto.Message
	handler  RequestChannelHandler
}

type dispatcher struct {
	Label             string
	lock              sync.RWMutex
	reqRespHandler    map[string]reqRespHandler
	reqChannelHandler map[string]reqChannelHandler
}

func newDispatcher(label string) dispatcher {
	return dispatcher{
		Label:             label,
		lock:              sync.RWMutex{},
		reqRespHandler:    make(map[string]reqRespHandler),
		reqChannelHandler: make(map[string]reqChannelHandler),
	}
}

func (r *dispatcher) FindReqRespHandler(key string) RequestResponseHandler {
	defer r.lock.RUnlock()
	r.lock.RLock()
	return r.reqRespHandler[key].handler
}

func (r *dispatcher) FindReqChannelHandler(key string) RequestChannelHandler {
	defer r.lock.RUnlock()
	r.lock.RLock()
	return r.reqChannelHandler[key].handler
}

func (r *dispatcher) registerRequestResponseHandler(key string, handler RequestResponseHandler) {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqRespHandler[key]; ok {
		return
	}
	r.reqRespHandler[key] = reqRespHandler{
		handler: handler,
	}
}

func (r *dispatcher) registerRequestChannelHandler(key string, handler RequestChannelHandler) {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqChannelHandler[key]; ok {
		return
	}
	r.reqChannelHandler[key] = reqChannelHandler{
		handler: handler,
	}
}