package entities

import (
	"time"

	"github.com/google/uuid"
)

// Message represents a message published to a topic.
type Message struct {
	Id        string            `json:"id"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers"`
	CreatedAt string            `json:"createdAt"`
}

// FillMissingFields fills the missing fields of a message, such as id, headers and created at.
func (message *Message) FillMissingFields() {
	message.Id = uuid.NewString()
	message.CreatedAt = time.Now().Format(time.RFC3339)

	if message.Headers == nil {
		message.Headers = make(map[string]string)
	}

}
