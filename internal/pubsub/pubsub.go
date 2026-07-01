// Package pubsub implements a small publish/subscribe broker: clients SUBSCRIBE
// to named channels, and PUBLISH fans a message out to every current subscriber
// of a channel.
//
// Concurrency design (this is the interesting part):
//   - The broker holds a map channel-name -> set of *Subscription. It is guarded
//     by a single RWMutex. Subscribe/Unsubscribe take the write lock (rare);
//     Publish takes the read lock (hot path) and snapshots the subscriber set.
//   - Each Subscription owns a buffered Go channel (chan Message). Publish does a
//     NON-BLOCKING send onto each subscriber's channel (select/default), so one
//     slow client can never stall the publisher or other subscribers. If a
//     subscriber's buffer is full, that message is dropped for that client —
//     the same "the network can't keep up" trade-off a real broker makes.
//   - The subscriber's connection goroutine ranges over its channel and writes
//     each Message to the client as a RESP push. Closing the channel (on full
//     unsubscribe / disconnect) ends that loop cleanly.
package pubsub

import "sync"

// Message is one published payload delivered to a subscriber.
type Message struct {
	Channel string
	Payload string
}

// Subscription is one client's handle for one channel. The connection goroutine
// receives on C; the broker sends on it.
type Subscription struct {
	Channel string
	C       chan Message
}

// Broker routes messages from publishers to subscribers.
type Broker struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscription]struct{} // channel -> set of subscriptions
}

// New returns an empty Broker ready for use.
func New() *Broker {
	return &Broker{subs: make(map[string]map[*Subscription]struct{})}
}

// Subscribe registers interest in channel and returns a Subscription whose C
// will receive future messages. Each call allocates a fresh buffered channel so
// subscriptions are independent.
func (b *Broker) Subscribe(channel string) *Subscription {
	sub := &Subscription{Channel: channel, C: make(chan Message, 64)}
	b.mu.Lock()
	set, ok := b.subs[channel]
	if !ok {
		set = make(map[*Subscription]struct{})
		b.subs[channel] = set
	}
	set[sub] = struct{}{}
	b.mu.Unlock()
	return sub
}

// Unsubscribe removes sub from channel and closes its delivery channel so the
// consuming goroutine's range loop terminates. Safe to call once per sub.
func (b *Broker) Unsubscribe(sub *Subscription) {
	b.mu.Lock()
	if set, ok := b.subs[sub.Channel]; ok {
		if _, present := set[sub]; present {
			delete(set, sub)
			close(sub.C)
			if len(set) == 0 {
				delete(b.subs, sub.Channel)
			}
		}
	}
	b.mu.Unlock()
}

// Publish delivers payload to every current subscriber of channel and returns
// how many subscribers received it. Delivery is non-blocking: a subscriber with
// a full buffer is skipped rather than blocking the publisher.
func (b *Broker) Publish(channel, payload string) int {
	b.mu.RLock()
	// Snapshot the subscriber set so we can release the lock before sending —
	// this keeps the critical section tiny and avoids holding the lock while a
	// (bounded) channel send happens.
	set := b.subs[channel]
	subs := make([]*Subscription, 0, len(set))
	for sub := range set {
		subs = append(subs, sub)
	}
	b.mu.RUnlock()

	delivered := 0
	msg := Message{Channel: channel, Payload: payload}
	for _, sub := range subs {
		select {
		case sub.C <- msg:
			delivered++
		default:
			// Subscriber's buffer is full; drop rather than block everyone else.
		}
	}
	return delivered
}
