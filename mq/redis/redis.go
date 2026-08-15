// Package redis implements a MailBaby queue publisher over Redis
// (streams, lists or pub/sub). The wire format mirrors the MailBaby server's
// redis driver exactly: every message is wrapped in a
// {"id","topic","payload","headers","timestamp","attempts"} envelope before
// being written, so an unmodified server can consume it.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mailbabys/mailbaby-go/mq"
)

// Supported Redis queue modes (mirror the server's queue.redis.mode).
const (
	ModeStream = "stream"
	ModeList   = "list"
	ModePubSub = "pubsub"
)

// Config configures the Redis producer. Field semantics match the MailBaby
// server's queue.redis configuration.
type Config struct {
	// Addr is the Redis address (host:port).
	Addr string
	// Password is the optional Redis password.
	Password string
	// DB is the Redis database number.
	DB int
	// Key is the destination stream/list key or pub/sub channel.
	Key string
	// Mode is "stream", "list" or "pubsub". Defaults to "stream".
	Mode string
	// MaxLen caps the stream length when Mode is "stream". 0 means unlimited.
	MaxLen int64
	// DialTimeout is the connection timeout. Defaults to 5 seconds.
	DialTimeout time.Duration
}

// Producer publishes email jobs to Redis in the MailBaby wire format.
type Producer struct {
	cfg    Config
	client *redis.Client
}

type envelope struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Attempts  int               `json:"attempts"`
}

// New creates a Redis Producer and verifies connectivity with a Ping.
func New(cfg Config) (*Producer, error) {
	if cfg.Addr == "" {
		return nil, errors.New("redis: addr is required")
	}
	if cfg.Key == "" {
		return nil, errors.New("redis: key is required")
	}

	mode := cfg.Mode
	if mode == "" {
		mode = ModeStream
	}
	switch mode {
	case ModeStream, ModeList, ModePubSub:
	default:
		return nil, fmt.Errorf("redis: unsupported mode %q", mode)
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  dialTimeout,
		ReadTimeout:  dialTimeout,
		WriteTimeout: dialTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return &Producer{cfg: cfg, client: client}, nil
}

// Publish sends a single message wrapped in the MailBaby envelope.
func (p *Producer) Publish(ctx context.Context, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("redis: message is nil")
	}
	if p.client == nil {
		return fmt.Errorf("redis: producer is closed")
	}

	key := p.cfg.Key
	if msg.Topic != "" {
		key = msg.Topic
	}

	env := envelope{
		ID:        msg.ID,
		Topic:     key,
		Payload:   msg.Payload,
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,
		Attempts:  msg.Attempts,
	}

	rawJSON, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("redis: failed to marshal envelope: %w", err)
	}

	publishAction := func() error {
		switch p.cfg.Mode {
		case ModeStream:
			args := &redis.XAddArgs{
				Stream: key,
				Values: map[string]any{
					"id":      env.ID,
					"payload": env.Payload,
					"data":    rawJSON,
				},
			}
			if p.cfg.MaxLen > 0 {
				args.MaxLen = p.cfg.MaxLen
				args.Approx = true
			}
			if err := p.client.XAdd(ctx, args).Err(); err != nil {
				return fmt.Errorf("redis: stream publish failed: %w", err)
			}
		case ModeList:
			if err := p.client.RPush(ctx, key, rawJSON).Err(); err != nil {
				return fmt.Errorf("redis: list publish failed: %w", err)
			}
		case ModePubSub:
			if err := p.client.Publish(ctx, key, rawJSON).Err(); err != nil {
				return fmt.Errorf("redis: pubsub publish failed: %w", err)
			}
		}
		return nil
	}

	if msg.Delay > 0 {
		go func(d time.Duration) {
			select {
			case <-time.After(d):
				_ = publishAction()
			case <-ctx.Done():
			}
		}(msg.Delay)
		return nil
	}

	return publishAction()
}

// Close closes the Redis client.
func (p *Producer) Close() error {
	if p.client == nil {
		return nil
	}
	err := p.client.Close()
	p.client = nil
	return err
}
