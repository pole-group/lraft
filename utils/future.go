// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"context"
	"sync/atomic"

	"github.com/jjeffcaii/reactor-go/mono"
	polerpc "github.com/pole-group/pole-rpc"
)

var DefaultScheduler *RoutinePool = NewRoutinePool(16, 128)

type RoutinePool struct {
	size         int32
	cacheSize    int32
	isRunning    int32
	taskChan     chan func()
	workers      []worker
	panicHandler func(err interface{})
}

func defaultPanicHandler(err interface{}) {
	RaftLog.Error("when exec user task occur panic : %#v", err)
}

func NewRoutinePool(size, cacheSize int32) *RoutinePool {
	pool := &RoutinePool{
		size:         size,
		isRunning:    1,
		cacheSize:    cacheSize,
		taskChan:     make(chan func(), cacheSize),
		panicHandler: defaultPanicHandler,
	}

	pool.init()
	return pool
}

func (rp *RoutinePool) SetPanicHandler(panicHandler func(err interface{})) {
	rp.panicHandler = panicHandler
}

func (rp *RoutinePool) init() {
	atomic.StoreInt32(&rp.isRunning, 1)
	workers := make([]worker, rp.size, rp.size)
	for i := int32(0); i < rp.size; i++ {
		workers[i] = worker{owner: rp}
		polerpc.GoEmpty(workers[i].run)
	}
}

func (rp *RoutinePool) Submit(task func()) {
	rp.taskChan <- task
}

func (rp *RoutinePool) Close() {
	atomic.StoreInt32(&rp.isRunning, 0)
	close(rp.taskChan)
}

type worker struct {
	owner *RoutinePool
}

func (w worker) run() {
	for task := range w.owner.taskChan {
		deal := func() {
			defer func() {
				if err := recover(); err != nil {
					w.owner.panicHandler(err)
				}
			}()
			task()
		}
		deal()
		if atomic.LoadInt32(&w.owner.isRunning) == int32(0) {
			return
		}
	}
}

type Future interface {
	run()
	Cancel()
}
type MonoFuture struct {
	ctx    context.Context
	cancel context.CancelFunc
	future mono.Mono
}

func NewMonoFuture(origin mono.Mono) *MonoFuture {
	ctx, cancel := context.WithCancel(context.Background())
	f := &MonoFuture{
		ctx:    ctx,
		cancel: cancel,
		future: origin,
	}
	f.run()
	return f
}

func (f *MonoFuture) run() {
	f.future.Subscribe(f.ctx)
}

func (f *MonoFuture) Cancel() () {
	f.cancel()
}
