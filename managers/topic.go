package managers

import (
	"errors"
	"sync"
	"time"

	"github.com/asynched/repl/channels"
	"github.com/asynched/repl/domain/entities"
)

var (
	ErrTopicNotFound error = errors.New("topic not found")
)

// Topic data structure that contains it's name and a channel for broadcasting messages.
type Topic struct {
	Name      string
	Broadcast *channels.Broadcast[*entities.Message]
}

// publish publishes a message to the topic.
func (topic *Topic) publish(message *entities.Message) {
	topic.Broadcast.Broadcast(message)
}

type TopicManager interface {
	CreateTopic(name string) bool
	Exists(topicName string) bool
	GetTopics() []string
	PublishMessage(topicName string, message *entities.Message) error
	Subscribe(topicName string) (chan *entities.Message, error)
	Unsubscribe(topicName string, listener chan *entities.Message)
}

// StandaloneTopicManager is a manager for topics inside the application.
type StandaloneTopicManager struct {
	lock   sync.RWMutex
	topics map[string]*Topic
}

// Exists checks if a topic exists.
// Returns true if the topic exists, false otherwise.
// Example:
//
//	manager.Exists("demo") // true
func (manager *StandaloneTopicManager) Exists(topicName string) bool {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	_, ok := manager.topics[topicName]

	return ok
}

// GetTopics returns a list of all topics.
func (manager *StandaloneTopicManager) GetTopics() []string {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	names := make([]string, 0)

	for name := range manager.topics {
		names = append(names, name)
	}

	return names
}

// CreateTopic tries to create a new topic and returns true
// if the topic was created successfully.
func (manager *StandaloneTopicManager) CreateTopic(name string) bool {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	if _, ok := manager.topics[name]; ok {
		return false
	}

	manager.topics[name] = &Topic{
		Name:      name,
		Broadcast: channels.NewBroadcast[*entities.Message](time.Second * 1),
	}

	return true
}

// PublishMessage publishes a message to a topic.
// Returns an error if the topic does not exist.
func (manager *StandaloneTopicManager) PublishMessage(topicName string, message *entities.Message) error {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return ErrTopicNotFound
	}

	message.FillMissingFields()

	topic.publish(message)

	return nil
}

// Subscribe subscribes to a topic.
// Returns a channel that will receive messages from the topic or an error if the topic does not exist.
func (manager *StandaloneTopicManager) Subscribe(topicName string) (chan *entities.Message, error) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return nil, ErrTopicNotFound
	}

	listener := make(chan *entities.Message)

	topic.Broadcast.AddListener(listener)

	return listener, nil
}

// Unsubscribe unsubscribes from a topic.
func (manager *StandaloneTopicManager) Unsubscribe(topicName string, listener chan *entities.Message) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return
	}

	topic.Broadcast.RemoveListener(listener)
}

// NewStandaloneTopicManager creates a new topic manager.
func NewStandaloneTopicManager() TopicManager {
	return &StandaloneTopicManager{
		lock:   sync.RWMutex{},
		topics: make(map[string]*Topic),
	}
}
