package channels

import "sync"

type Broker[T any] struct {
	lock       sync.Mutex
	listeners  map[chan T]struct{}
	register   chan chan T
	unregister chan chan T
	messages   chan T
}

func NewBroker[T any]() *Broker[T] {
	return &Broker[T]{
		listeners:  make(map[chan T]struct{}),
		register:   make(chan chan T),
		unregister: make(chan chan T),
		messages:   make(chan T),
		lock:       sync.Mutex{},
	}
}

func (b *Broker[T]) AddListener() chan T {
	listener := make(chan T)
	b.register <- listener
	return listener
}

func (b *Broker[T]) RemoveListener(listener chan T) {
	b.unregister <- listener
}

func (b *Broker[T]) SendMessage(message T) {
	b.messages <- message
}

func (b *Broker[T]) Run() {
	for {
		select {
		case listener := <-b.register:
			b.lock.Lock()
			b.listeners[listener] = struct{}{}
			b.lock.Unlock()

		case listener := <-b.unregister:
			b.lock.Lock()

			delete(b.listeners, listener)
			close(listener)

			b.lock.Unlock()
		case message := <-b.messages:
			b.lock.Lock()

			for listener := range b.listeners {
				listener <- message
			}

			b.lock.Unlock()
		}
	}
}
