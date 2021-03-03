// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"sync"
)

type dispatcher struct {
	Label             string
	lock              sync.RWMutex
	reqRespHandler    map[string]RequestResponseHandler
	reqChannelHandler map[string]RequestChannelHandler
}

func newDispatcher(label string) dispatcher {
	return dispatcher{
		Label:             label,
		lock:              sync.RWMutex{},
		reqRespHandler:    make(map[string]RequestResponseHandler),
		reqChannelHandler: make(map[string]RequestChannelHandler),
	}
}

func (r *dispatcher) FindReqRespHandler(key string) RequestResponseHandler {
	defer r.lock.RUnlock()
	r.lock.RLock()
	return r.reqRespHandler[key]
}

func (r *dispatcher) FindReqChannelHandler(key string) RequestChannelHandler {
	defer r.lock.RUnlock()
	r.lock.RLock()
	return r.reqChannelHandler[key]
}

func (r *dispatcher) registerRequestResponseHandler(key string, handler RequestResponseHandler) bool {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqRespHandler[key]; ok {
		return false
	}
	r.reqRespHandler[key] = handler
	return true
}

func (r *dispatcher) registerRequestChannelHandler(key string, handler RequestChannelHandler) bool {
	defer r.lock.Unlock()
	r.lock.Lock()

	if _, ok := r.reqChannelHandler[key]; ok {
		return false
	}
	r.reqChannelHandler[key] = handler
	return true
}
