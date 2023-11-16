package managers

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/asynched/repl/channels"
	"github.com/asynched/repl/domain/entities"
	"github.com/hashicorp/raft"
)

var (
	ErrTopicNotFound      error = errors.New("topic not found")
	ErrTopicAlreadyExists error = errors.New("topic already exists")
)

type TopicManager interface {
	CreateTopic(name string) error
	Exists(topicName string) bool
	GetTopics() []string
	PublishMessage(topicName string, message entities.Message) error
	Subscribe(topicName string) (chan entities.Message, error)
	Unsubscribe(topicName string, listener chan entities.Message)
}

// Topic data structure that contains it's name and a channel for broadcasting messages.
type Topic struct {
	Name   string
	broker *channels.Broker[entities.Message]
}

// publish publishes a message to the topic.
func (topic *Topic) publish(message entities.Message) {
	topic.broker.Publish(message)
}

func NewTopic(name string) *Topic {
	broker := channels.NewBroker[entities.Message]()

	go broker.Run(time.Millisecond * 10)

	return &Topic{
		Name:   name,
		broker: broker,
	}
}

// StandaloneTopicManager is a manager for topics inside the application.
type StandaloneTopicManager struct {
	lock   sync.RWMutex
	logger *log.Logger
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
func (manager *StandaloneTopicManager) CreateTopic(name string) error {
	manager.lock.Lock()
	defer manager.lock.Unlock()

	if _, ok := manager.topics[name]; ok {
		return ErrTopicAlreadyExists
	}

	manager.topics[name] = NewTopic(name)

	return nil
}

// PublishMessage publishes a message to a topic.
// Returns an error if the topic does not exist.
func (manager *StandaloneTopicManager) PublishMessage(topicName string, message entities.Message) error {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return ErrTopicNotFound
	}

	topic.publish(message)

	return nil
}

// Subscribe subscribes to a topic.
// Returns a channel that will receive messages from the topic or an error if the topic does not exist.
func (manager *StandaloneTopicManager) Subscribe(topicName string) (chan entities.Message, error) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return nil, ErrTopicNotFound
	}

	listener := topic.broker.Subscribe()

	return listener, nil
}

// Unsubscribe unsubscribes from a topic.
func (manager *StandaloneTopicManager) Unsubscribe(topicName string, listener chan entities.Message) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return
	}

	topic.broker.Remove(listener)
}

func (manager *StandaloneTopicManager) RunStats() {
	for {
		manager.lock.RLock()

		for name, topic := range manager.topics {
			manager.logger.Printf("event='stats' topic='%s' clients=%d messages=%d\n", name, topic.broker.ClientCount(), topic.broker.MessageCount())
		}

		manager.lock.RUnlock()

		time.Sleep(5 * time.Second)
	}
}

// NewStandaloneTopicManager creates a new topic manager.
func NewStandaloneTopicManager() *StandaloneTopicManager {
	logger := log.New(os.Stdout, "[manager] ", log.Flags())

	manager := &StandaloneTopicManager{
		lock:   sync.RWMutex{},
		logger: logger,
		topics: make(map[string]*Topic),
	}

	go manager.RunStats()

	return manager
}

type RaftTopicManager struct {
	raft   *raft.Raft
	lock   sync.RWMutex
	topics map[string]*Topic
}

func NewRaftTopicManager() *RaftTopicManager {
	return &RaftTopicManager{
		lock:   sync.RWMutex{},
		topics: make(map[string]*Topic),
	}
}

func (manager *RaftTopicManager) Configure(raft *raft.Raft) {
	manager.raft = raft
}

func (manager *RaftTopicManager) CreateTopic(name string) error {
	cmd := &raftCreateTopicCommand{
		Kind: raftCommandKindCreateTopic,
		Name: name,
	}

	data, err := json.Marshal(cmd)

	if err != nil {
		return err
	}

	f := manager.raft.Apply(data, 5*time.Second)

	return f.Error()
}

func (manager *RaftTopicManager) Exists(topicName string) bool {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	_, ok := manager.topics[topicName]

	return ok
}

func (manager *RaftTopicManager) GetTopics() []string {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	names := make([]string, 0)

	for name := range manager.topics {
		names = append(names, name)
	}

	return names
}

func (manager *RaftTopicManager) PublishMessage(topicName string, message entities.Message) error {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	_, ok := manager.topics[topicName]

	if !ok {
		return ErrTopicNotFound
	}

	cmd := &raftPublishMessageCommand{
		Kind:    raftCommandKindPublishMessage,
		Topic:   topicName,
		Message: message,
	}

	data, err := json.Marshal(cmd)

	if err != nil {
		return err
	}

	f := manager.raft.Apply(data, 5*time.Second)

	return f.Error()
}

func (manager *RaftTopicManager) Subscribe(topicName string) (chan entities.Message, error) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return nil, ErrTopicNotFound
	}

	listener := topic.broker.Subscribe()

	return listener, nil
}

func (manager *RaftTopicManager) Unsubscribe(topicName string, listener chan entities.Message) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	topic, ok := manager.topics[topicName]

	if !ok {
		return
	}

	topic.broker.Remove(listener)
}

type raftCommandKind int

const (
	raftCommandKindCreateTopic    raftCommandKind = 1
	raftCommandKindPublishMessage raftCommandKind = 2
)

type raftCommand struct {
	Kind raftCommandKind `json:"kind"`
}

type raftCreateTopicCommand struct {
	Kind raftCommandKind `json:"kind"`
	Name string          `json:"name"`
}

type raftPublishMessageCommand struct {
	Kind    raftCommandKind  `json:"kind"`
	Topic   string           `json:"topic"`
	Message entities.Message `json:"message"`
}

func (manager *RaftTopicManager) Apply(l *raft.Log) interface{} {
	command := &raftCommand{}

	if err := json.Unmarshal(l.Data, command); err != nil {
		log.Fatalf("failed to parse raft command: %s", err)
	}

	switch command.Kind {
	case raftCommandKindCreateTopic:
		manager.lock.Lock()
		defer manager.lock.Unlock()

		createTopicCommand := &raftCreateTopicCommand{}

		if err := json.Unmarshal(l.Data, createTopicCommand); err != nil {
			log.Fatalf("failed to parse raft command: %s", err)
		}

		manager.topics[createTopicCommand.Name] = NewTopic(createTopicCommand.Name)
		return nil
	case raftCommandKindPublishMessage:
		publishMessageCommand := &raftPublishMessageCommand{}

		if err := json.Unmarshal(l.Data, publishMessageCommand); err != nil {
			log.Fatalf("failed to parse raft command: %s", err)
		}

		topic, ok := manager.topics[publishMessageCommand.Topic]

		if !ok {
			log.Fatalf("topic not found: %s", publishMessageCommand.Topic)
		}

		topic.publish(publishMessageCommand.Message)
	default:
		log.Fatalf("unknown raft command kind: %d", command.Kind)
	}

	return nil
}

type topicManagerSnapshot struct {
	Topics map[string]string
}

func (snapshot *topicManagerSnapshot) Persist(sink raft.SnapshotSink) error {
	encoder := gob.NewEncoder(sink)

	if err := encoder.Encode(snapshot); err != nil {
		return err
	}

	if err := sink.Close(); err != nil {
		return err
	}

	return nil
}

func (snapshot *topicManagerSnapshot) Release() {}

func (manager *RaftTopicManager) Snapshot() (raft.FSMSnapshot, error) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()

	snapshot := &topicManagerSnapshot{
		Topics: make(map[string]string),
	}

	for name, topic := range manager.topics {
		snapshot.Topics[name] = topic.Name
	}

	return snapshot, nil
}

func (manager *RaftTopicManager) Restore(rc io.ReadCloser) error {
	snapshot := &topicManagerSnapshot{}

	if err := gob.NewDecoder(rc).Decode(snapshot); err != nil {
		return err
	}

	manager.lock.Lock()
	defer manager.lock.Unlock()

	for name, topicName := range snapshot.Topics {
		manager.topics[name] = NewTopic(topicName)
	}

	return nil
}
