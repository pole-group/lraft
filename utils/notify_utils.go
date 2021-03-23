// Copyright (c) 2020, pole-group. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package utils

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	polerpc "github.com/pole-group/pole-rpc"
)

var (
	ErrorEventNotRegister     = errors.New("the event was not registered")
	ErrorEventRegister        = errors.New("register event publisher failed")
	ErrorAddSubscriber        = errors.New("add subscriber failed")
	publisherCenter           *PublisherCenter
	publisherOnce             sync.Once
	defaultFastRingBufferSize = GetInt64FromEnvOptional("lraft.notify.fast-event-buffer.size", 16384)
)

type PublisherCenter struct {
	Publishers    *polerpc.ConcurrentMap // <string:event_name, *Publisher >
	hasSubscriber bool
	// just for test
	onExpire func(event Event)
}

func InitPublisherCenter() {
	publisherOnce.Do(func() {
		publisherCenter = &PublisherCenter{
			Publishers:    &polerpc.ConcurrentMap{},
			hasSubscriber: false,
		}
	})
}

func RegisterPublisherDefault(ctx context.Context, event Event) error {
	return RegisterPublisher(ctx, event, defaultFastRingBufferSize)
}

func RegisterPublisher(ctx context.Context, event Event, ringBufferSize int64) error {
	if ringBufferSize <= 32 {
		ringBufferSize = 128
	}
	topic := event.Name()

	publisherCenter.Publishers.ComputeIfAbsent(topic, func() interface{} {
		return &Publisher{
			queue:        make(chan eventHolder, ringBufferSize),
			topic:        topic,
			subscribers:  &sync.Map{},
			lastSequence: -1,
			ctx:          ctx,
		}
	})

	p := publisherCenter.Publishers.Get(topic)

	if p == nil {
		return ErrorEventRegister
	}
	publisher := p.(*Publisher)
	publisher.start()
	return nil
}

func PublishEvent(event ...Event) error {
	if p := publisherCenter.Publishers.Get(event[0].Name()); p != nil {
		p.(*Publisher).PublishEvent(event...)
		return nil
	}
	return ErrorEventNotRegister
}

func PublishEventNonBlock(event ...Event) (bool, error) {
	if p := publisherCenter.Publishers.Get(event[0].Name()); p != nil {
		return p.(*Publisher).PublishEventNonBlock(event...), nil
	}
	return false, ErrorEventNotRegister
}

func RegisterSubscriber(s Subscriber) error {
	topic := s.SubscribeType()
	if v := publisherCenter.Publishers.Get(topic.Name()); v != nil {
		p := v.(*Publisher)
		(*p).AddSubscriber(s)
		return nil
	}
	return fmt.Errorf("this topic [%s] no publisher", topic)
}

func DeregisterSubscriber(s Subscriber) {
	topic := s.SubscribeType()
	if v := publisherCenter.Publishers.Get(topic.Name()); v != nil {
		p := v.(*Publisher)
		(*p).RemoveSubscriber(s)
	}
}

func Shutdown() {
	publisherCenter.Publishers.ForEach(func(k, v interface{}) {
		p := v.(*Publisher)
		(*p).shutdown()
	})
}

func TestRegisterOnExpire(f func(event Event)) {
	publisherCenter.onExpire = f
}

// Event interface
type Event interface {
	// Topic of the event
	Name() string
	// The sequence number of the event
	Sequence() int64
}

type eventHolder struct {
	events []Event
}

type Subscriber interface {
	OnEvent(event Event, endOfBatch bool)

	IgnoreExpireEvent() bool

	SubscribeType() Event
}

type Publisher struct {
	queue        chan eventHolder
	topic        string
	subscribers  *sync.Map
	init         sync.Once
	canOpen      bool
	isClosed     bool
	lastSequence int64
	ctx          context.Context
}

func (p *Publisher) start() {
	p.init.Do(func() {
		go p.openHandler()
	})
}

func (p *Publisher) PublishEvent(event ...Event) {
	if p.isClosed {
		return
	}
	p.queue <- eventHolder{
		events: event,
	}
}

func (p *Publisher) PublishEventNonBlock(events ...Event) bool {
	if p.isClosed {
		return false
	}
	select {
	case p.queue <- eventHolder{
		events: events,
	}:
		return true
	default:
		return false
	}
}

func (p *Publisher) PublishEventWithTimeout(timeout time.Duration,
	events ...Event) (bool, error) {
	if p.isClosed {
		return false, fmt.Errorf("this publisher already close")
	}
	t := time.NewTimer(timeout)
	select {
	case p.queue <- eventHolder{
		events: events,
	}:
		return true, nil
	case <-t.C:
		return false, fmt.Errorf("publish event timeout")
	}
}

func (p *Publisher) AddSubscriber(s Subscriber) {
	p.subscribers.Store(s, member)
	p.canOpen = true
}

func (p *Publisher) RemoveSubscriber(s Subscriber) {
	p.subscribers.Delete(s)
}

func (p *Publisher) shutdown() {
	if p.isClosed {
		return
	}
	p.isClosed = true
	close(p.queue)
	p.subscribers = nil
}

func (p *Publisher) openHandler() {

	defer func() {
		if err := recover(); err != nil {
			RaftLog.Error("dispose fast event has error : %s", err)
		}
	}()

	for {
		if p.canOpen {
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}

	for {
		select {
		case e := <-p.queue:
			RaftLog.Debug("handler receive fast event : %s", e)
			p.notifySubscriber(e)
		case <-p.ctx.Done():
			p.shutdown()
			return
		}
	}
}

func (p *Publisher) notifySubscriber(events eventHolder) {
	currentSequence := p.lastSequence
	es := events.events
	s := len(es) - 1
	for i, e := range es {
		currentSequence = e.Sequence()
		p.subscribers.Range(func(key, value interface{}) bool {

			defer func() {
				if err := recover(); err != nil {
					RaftLog.Error("notify subscriber has error : %s", err)
				}
			}()

			subscriber := key.(Subscriber)

			if subscriber.IgnoreExpireEvent() && currentSequence < p.lastSequence {
				// just for test
				if publisherCenter.onExpire != nil {
					publisherCenter.onExpire(e)
				}
				return true
			}

			subscriber.OnEvent(e, i == s)
			return true
		})
	}

	p.lastSequence = int64(math.Max(float64(currentSequence), float64(p.lastSequence)))
}
