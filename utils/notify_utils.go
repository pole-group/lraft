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
)

var (
	ErrorEventNotRegister = errors.New("the event was not registered")
	ErrorEventRegister    = errors.New("register event publisher failed")
	ErrorAddSubscriber    = errors.New("add subscriber failed")
	publisherCenter       *PublisherCenter
	publisherOnce         sync.Once

	defaultFastRingBufferSize = GetInt64FromEnvOptional("github.com/pole-group/lraft.notify.fast-event-buffer.size", 16384)
)

type PublisherCenter struct {
	Publishers    sync.Map
	hasSubscriber bool
	// just for test
	onExpire func(event Event)
}

func InitPublisherCenter() {
	publisherOnce.Do(func() {
		publisherCenter = &PublisherCenter{
			Publishers:    sync.Map{},
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

	publisherCenter.Publishers.LoadOrStore(topic, &Publisher{
		queue:        make(chan eventHolder, ringBufferSize),
		topic:        topic,
		subscribers:  &sync.Map{},
		lastSequence: -1,
		ctx:          ctx,
	})

	p, ok := publisherCenter.Publishers.Load(topic)

	if ok {
		publisher := p.(*Publisher)
		publisher.start()

		return nil
	}
	return ErrorEventRegister

}

func PublishEvent(event ...Event) error {
	if p, ok := publisherCenter.Publishers.Load(event[0].Name()); ok {
		p.(*Publisher).PublishEvent(event...)
		return nil
	}
	return ErrorEventNotRegister
}

func PublishEventNonBlock(event ...Event) (bool, error) {
	if p, ok := publisherCenter.Publishers.Load(event[0].Name()); ok {
		return p.(*Publisher).PublishEventNonBlock(event...), nil
	}
	return false, ErrorEventNotRegister
}

func RegisterSubscriber(s Subscriber) error {
	topic := s.SubscribeType()
	if v, ok := publisherCenter.Publishers.Load(topic.Name()); ok {
		p := v.(*Publisher)
		(*p).AddSubscriber(s)
		return nil
	}
	return fmt.Errorf("this topic [%s] no publisher", topic)
}

func DeregisterSubscriber(s Subscriber) {
	topic := s.SubscribeType()
	if v, ok := publisherCenter.Publishers.Load(topic); ok {
		p := v.(*Publisher)
		(*p).RemoveSubscriber(s)
	}
}

func Shutdown() {
	publisherCenter.Publishers.Range(func(key, value interface{}) bool {
		p := key.(*Publisher)
		(*p).shutdown()
		return true
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
	if !p.isClosed {
		return
	}
	p.queue <- eventHolder{
		events: event,
	}
}

func (p *Publisher) PublishEventNonBlock(events ...Event) bool {
	if !p.isClosed {
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
			fmt.Printf("dispose fast event has error : %s\n", err)
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
			fmt.Printf("handler receive fast event : %s\n", e)
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
					fmt.Printf("notify subscriber has error : %s \n", err)
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
