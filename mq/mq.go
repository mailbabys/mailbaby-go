// Package mq provides message-queue publishing primitives for MailBaby.
// The wire contract mirrors the MailBaby server's queue subsystem so that
// emails published with this package are consumed by an unmodified server.
package mq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mailbaby "github.com/mailbabys/mailbaby-go"
)

// Message represents a queue message for publishing. Its fields mirror the
// server-side queue.Message wire contract (publishing subset).
type Message struct {
	// ID is the unique identifier of the message.
	ID string

	// Topic is the destination queue/topic/routing key/stream.
	Topic string

	// Payload is the raw byte payload of the message. For email jobs this
	// is the JSON serialization of a mailbaby.Email.
	Payload []byte

	// Headers contains metadata key-value pairs associated with the message.
	Headers map[string]string

	// Key is an optional partition key / sharding key (Kafka).
	Key string

	// Delay specifies the duration after which the message becomes eligible
	// for delivery (supported by the Redis driver).
	Delay time.Duration

	// Timestamp records when the message was generated.
	Timestamp time.Time

	// Attempts records how many times this message has been delivered.
	Attempts int

	mu sync.Mutex
}

// NewMessage creates a Message with a randomly generated ID and the current
// timestamp.
func NewMessage(payload []byte) *Message {
	return NewMessageWithID(generateID(), payload)
}

// NewMessageWithID creates a Message with a specific ID and payload.
func NewMessageWithID(id string, payload []byte) *Message {
	if id == "" {
		id = generateID()
	}
	return &Message{
		ID:        id,
		Payload:   payload,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
		Attempts:  1,
	}
}

// NewEmailMessage builds a queue message from a validated email. The payload
// is the email's MailBaby wire-format JSON, and the message ID defaults to
// the email ID (or a generated one when empty).
func NewEmailMessage(email *mailbaby.Email) (*Message, error) {
	if err := email.Validate(); err != nil {
		return nil, err
	}
	data, err := email.ToJSON()
	if err != nil {
		return nil, fmt.Errorf("mq: failed to serialize email: %w", err)
	}
	msg := NewMessageWithID(email.ID, data)
	msg.SetHeader("Content-Type", "application/json")
	return msg, nil
}

// BindJSON deserializes the Message payload into the provided pointer.
func (m *Message) BindJSON(v any) error {
	if m == nil || len(m.Payload) == 0 {
		return fmt.Errorf("mq: message is nil or empty")
	}
	return json.Unmarshal(m.Payload, v)
}

// SetHeader sets a key-value header in the message.
func (m *Message) SetHeader(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
}

// GetHeader retrieves a header value by key.
func (m *Message) GetHeader(key string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Headers == nil {
		return ""
	}
	return m.Headers[key]
}

// Publisher is the contract implemented by every queue driver in this
// package. Each driver publishes the message in the exact wire format the
// MailBaby server consumes.
type Publisher interface {
	// Publish sends a single message to the queue/topic.
	Publish(ctx context.Context, msg *Message) error

	// Close releases underlying connections and resources.
	Close() error
}

// PublishEmail validates and publishes an email to the given topic in one
// call, returning the published message (with its generated ID) for
// reference.
func PublishEmail(ctx context.Context, p Publisher, topic string, email *mailbaby.Email) (*Message, error) {
	if p == nil {
		return nil, fmt.Errorf("mq: publisher is nil")
	}
	msg, err := NewEmailMessage(email)
	if err != nil {
		return nil, err
	}
	msg.Topic = topic
	if err := p.Publish(ctx, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func generateID() string {
	var buf [16]byte
	_, _ = rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
