package channels

import (
	"sync"
	"time"
)

// Broadcast is a channel that broadcasts a value to all listeners.
// The timeout parameter is used to prevent blocking the broadcast channel.
// If a listener is not ready to receive a value within the timeout period,
// the broadcast will skip the listener.
type Broadcast[T any] struct {
	lock      sync.RWMutex
	timeout   time.Duration
	listeners map[chan T]struct{}
}

// AddListener adds a listener to the broadcast channel.
func (broadcast *Broadcast[T]) AddListener(listener chan T) {
	broadcast.lock.Lock()
	defer broadcast.lock.Unlock()

	broadcast.listeners[listener] = struct{}{}
}

// RemoveListener removes a listener from the broadcast channel.
func (broadcast *Broadcast[T]) RemoveListener(listener chan T) {
	broadcast.lock.Lock()
	defer broadcast.lock.Unlock()

	delete(broadcast.listeners, listener)
	close(listener)
}

// Broadcast sends a value to all listeners.
func (broadcast *Broadcast[T]) Broadcast(value T) {
	broadcast.lock.RLock()
	defer broadcast.lock.RUnlock()

	timer := time.NewTimer(broadcast.timeout)

	for listener := range broadcast.listeners {
		select {
		case listener <- value:
		case <-timer.C:
			timer.Reset(broadcast.timeout)
			continue
		}
	}
}

// NewBroadcast creates a new broadcast channel.
// The timeout parameter is used to prevent blocking the broadcast channel.
// If a listener is not ready to receive a value within the timeout period,
// the broadcast will skip the listener.
//
//	broadcast := NewBroadcast[string](time.Second * 1)
//	listener := make(chan string)
//
//	go func () {
//		for { fmt.Println(<-listener) }
//	}()
//
//	broadcast.AddListener(listener)
//	broadcast.Broadcast("Hi!")
func NewBroadcast[T any](timeout time.Duration) *Broadcast[T] {
	return &Broadcast[T]{
		timeout:   timeout,
		listeners: make(map[chan T]struct{}),
	}
}
