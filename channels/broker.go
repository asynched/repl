package channels

import (
	"sync"
	"time"
)

type node[T any] struct {
	value T
	next  *node[T]
}

type queue[T any] struct {
	head *node[T]
	tail *node[T]
	size uint64
}

func newQueue[T any]() *queue[T] {
	return &queue[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (q *queue[T]) enqueue(v T) {
	n := &node[T]{
		value: v,
		next:  nil,
	}

	if q.head == nil {
		q.head = n
		q.tail = n
	} else {
		q.tail.next = n
		q.tail = n
	}

	q.size++
}

func (q *queue[T]) dequeue() *T {
	if q.head == nil {
		return nil
	}

	n := q.head
	q.head = q.head.next

	if q.head == nil {
		q.tail = nil
	}

	q.size--

	return &n.value
}

func (q *queue[T]) dequeueN(n uint64) []T {
	items := make([]T, 0)

	if q.size < n {
		return items
	}

	for i := uint64(0); i < n; i++ {
		items = append(items, *q.dequeue())
	}

	return items
}

// Broker is a channel that broadcasts a value to all listeners.
type Broker[T any] struct {
	lock     sync.RWMutex
	channels map[chan T]struct{}
	queue    *queue[T]
}

// NewBroker creates a new broker channel.
// A broker is a primitive structure to synchronize
// and send messages to multiple channels.
func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		lock:     sync.RWMutex{},
		channels: make(map[chan T]struct{}),
		queue:    newQueue[T](),
	}
}

// Subscribe adds a listener to the broker channel.
// The listener channel will receive all values published to the broker.
func (broker *Broker[T]) Subscribe() chan T {
	broker.lock.Lock()
	defer broker.lock.Unlock()

	channel := make(chan T)

	broker.channels[channel] = struct{}{}

	return channel
}

// Unsubscribe removes a listener from the broker channel.
func (broker *Broker[T]) Remove(channel chan T) {
	broker.lock.Lock()
	defer broker.lock.Unlock()

	delete(broker.channels, channel)
}

// Publish sends a value to all listeners.
func (broker *Broker[T]) Publish(value T) {
	broker.lock.Lock()
	defer broker.lock.Unlock()
	broker.queue.enqueue(value)
}

func (broker *Broker[T]) ClientCount() uint64 {
	broker.lock.RLock()
	defer broker.lock.RUnlock()

	return uint64(len(broker.channels))
}

func (broker *Broker[T]) MessageCount() uint64 {
	broker.lock.RLock()
	defer broker.lock.RUnlock()

	return broker.queue.size
}

// Run starts the broker channel.
// The provided value for tick is the interval
// between each message sent to the listeners.
// If the broker has no messages to send,
// it will wait until a message is published.
//
// Example:
//
//	broker := NewBroker[string]()
//	listener := broker.Subscribe()
//
//	go broker.Run(time.Second * 1)
//
//	broker.Publish("Hello, World!")
//	fmt.Println(<-listener)
func (broker *Broker[T]) Run(tick time.Duration) {

	for {
		broker.lock.Lock()
		message := broker.queue.dequeue()

		if message == nil {
			broker.lock.Unlock()
			continue
		}

		for channel := range broker.channels {
			channel <- *message
		}

		broker.lock.Unlock()

		time.Sleep(tick)
	}
}
