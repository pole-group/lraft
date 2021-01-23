// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package pole_rpc

import (
	"container/list"
	"context"
	"sync"
	"time"
)

func GoEmpty(work func()) {
	go work()
}

func Go(ctx context.Context, work func(ctx context.Context)) {
	go work(ctx)
}

func DoTimerSchedule(ctx context.Context, work func(), delay time.Duration, supplier func() time.Duration) {
	go func(ctx context.Context) {
		timer := time.NewTimer(delay)
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
				work()
				timer.Reset(supplier())
			}
		}
	}(ctx)
}

func DoTickerSchedule(ctx context.Context, work func(), delay time.Duration) {
	go func(ctx context.Context) {
		ticker := time.NewTicker(delay)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
			case <-ticker.C:
				work()
			}
		}

	}(ctx)
}

type TimeFunction interface {
	Run()
}

type HashTimeWheel struct {
	rwLock     sync.RWMutex
	buckets    []*timeBucket
	timeTicker *time.Ticker
	interval   time.Duration
	topSign    chan bool
	tick       int64
	slotNum    int32
}

func NewTimeWheel(interval time.Duration, slotNum int32) *HashTimeWheel {
	htw := &HashTimeWheel{
		rwLock:     sync.RWMutex{},
		interval:   interval,
		timeTicker: time.NewTicker(interval),
		topSign:    make(chan bool),
		buckets:    make([]*timeBucket, slotNum, slotNum),
	}

	for i := int32(0); i < slotNum; i ++ {
		htw.buckets[i] = newTimeBucket()
	}
	return htw
}

func (htw *HashTimeWheel) Start() {
	Go(context.Background(), func(ctx context.Context) {
		for {
			select {
			case <-htw.timeTicker.C:
				htw.process()
			case <-htw.topSign:
				return
			}
		}
	})
}

func (htw *HashTimeWheel) AddTask(f TimeFunction, delay time.Duration)  {
	pos, circle := htw.getSlots(delay)
	task := timeTask{
		circle: circle,
		f:      f,
		delay:  delay,
	}
	bucket := htw.buckets[pos]
	bucket.addUserTask(task)
}

func (htw *HashTimeWheel) Stop() {
	htw.topSign <- true
	htw.clearAllAndProcess()
}

func (htw *HashTimeWheel) process() {
	currentBucket := htw.buckets[htw.tick]
	htw.scanExpireAndRun(currentBucket)
	htw.tick = (htw.tick + 1) % int64(htw.slotNum)
}

func (htw *HashTimeWheel) clearAllAndProcess()  {
	for i := htw.slotNum; i > 0; i -- {
		htw.process()
	}
}

type timeBucket struct {
	rwLock sync.RWMutex
	queue  *list.List
	worker chan timeTask
}

func (tb *timeBucket) addUserTask(t timeTask) {
	defer tb.rwLock.Unlock()
	tb.rwLock.Lock()
	tb.queue.PushBack(t)
}

func (tb *timeBucket) execUserTask() {
	for task := range tb.worker {
		task.f.Run()
	}
}

func newTimeBucket() *timeBucket {
	tb := &timeBucket{
		rwLock: sync.RWMutex{},
		queue:  list.New(),
		worker: make(chan timeTask, 32),
	}
	GoEmpty(func() {
		tb.execUserTask()
	})
	return tb
}

func (htw *HashTimeWheel) scanExpireAndRun(tb *timeBucket) {
	defer tb.rwLock.Unlock()
	tb.rwLock.Lock()
	execCnt := 0
	for item := tb.queue.Front(); item != nil; {
		task := item.Value.(timeTask)
		if task.circle < 0 {
			timeout := time.After(htw.interval)
			select {
			case tb.worker <- task:
				execCnt++
				next := item.Next()
				tb.queue.Remove(item)
				item = next
			case <-timeout:
				item = item.Next()
				continue
			}
		} else {
			task.circle -= 1
			item = item.Next()
			continue
		}
	}
}

type timeTask struct {
	circle int32
	f      TimeFunction
	delay  time.Duration
}

func (htw *HashTimeWheel) getSlots(d time.Duration) (pos int32, circle int32) {
	delayTime := int64(d.Seconds())
	interval := int64(htw.interval.Seconds())
	return int32(htw.tick+delayTime/interval) % htw.slotNum, int32(delayTime / interval / int64(htw.slotNum))
}


