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
	listeners []chan T
}

// AddListener adds a listener to the broadcast channel.
func (broadcast *Broadcast[T]) AddListener(listener chan T) {
	broadcast.lock.Lock()
	defer broadcast.lock.Unlock()
	broadcast.listeners = append(broadcast.listeners, listener)
}

// RemoveListener removes a listener from the broadcast channel.
func (broadcast *Broadcast[T]) RemoveListener(listener chan T) {
	broadcast.lock.Lock()
	defer broadcast.lock.Unlock()

	for index, l := range broadcast.listeners {
		if l == listener {
			broadcast.listeners = append(broadcast.listeners[:index], broadcast.listeners[index+1:]...)
			return
		}
	}
}

// Broadcast sends a value to all listeners.
func (broadcast *Broadcast[T]) Broadcast(value T) {
	broadcast.lock.RLock()
	defer broadcast.lock.RUnlock()

	for _, listener := range broadcast.listeners {
		go func(listener chan T) {
			select {
			case listener <- value:
				return
			case <-time.After(broadcast.timeout):
				return
			}
		}(listener)
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
		listeners: make([]chan T, 0),
	}
}
