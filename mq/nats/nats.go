// Package nats implements a MailBaby queue publisher over NATS (core NATS
// or JetStream). The publishing behavior mirrors the MailBaby server's nats
// driver: the message payload is sent as the message data, headers are
// attached as NATS headers, and the message ID is propagated via the
// Nats-Msg-Id header.
package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"

	"github.com/mailbabys/mailbaby-go/mq"
)

// Config configures the NATS producer. Field semantics match the MailBaby
// server's queue.nats configuration.
type Config struct {
	// URL is the NATS server URL (e.g. nats://localhost:4222).
	URL string
	// Subject is the destination subject.
	Subject string
	// JetStream routes publishes through the JetStream API instead of core
	// NATS.
	JetStream bool
	// Name is the optional connection name.
	Name string
	// User and Password provide optional credential authentication.
	User     string
	Password string
	// Token provides optional token authentication.
	Token string
	// MaxReconnects is the maximum reconnect attempts. Defaults to 10.
	MaxReconnects int
	// ReconnectWait is the delay between reconnect attempts. Defaults to
	// 2 seconds.
	ReconnectWait time.Duration
	// ConnectTimeout is the connection timeout. Defaults to 5 seconds.
	ConnectTimeout time.Duration
}

// Producer publishes email jobs to a NATS subject in the MailBaby wire
// format.
type Producer struct {
	cfg Config
	nc  *natsgo.Conn
	js  natsgo.JetStreamContext
}

// New creates a NATS Producer and establishes the connection.
func New(cfg Config) (*Producer, error) {
	if cfg.URL == "" {
		return nil, errors.New("nats: url is required")
	}
	if cfg.Subject == "" {
		return nil, errors.New("nats: subject is required")
	}

	opts := []natsgo.Option{
		natsgo.Name(cfg.Name),
		natsgo.MaxReconnects(cfg.MaxReconnects),
		natsgo.ReconnectWait(reconnectWait(cfg.ReconnectWait)),
		natsgo.Timeout(connectTimeout(cfg.ConnectTimeout)),
	}
	if cfg.User != "" || cfg.Password != "" {
		opts = append(opts, natsgo.UserInfo(cfg.User, cfg.Password))
	}
	if cfg.Token != "" {
		opts = append(opts, natsgo.Token(cfg.Token))
	}

	nc, err := natsgo.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect failed: %w", err)
	}

	p := &Producer{cfg: cfg, nc: nc}
	if cfg.JetStream {
		js, err := nc.JetStream()
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("nats: jetstream setup failed: %w", err)
		}
		p.js = js
	}
	return p, nil
}

// Publish sends a single message to the configured subject.
func (p *Producer) Publish(ctx context.Context, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("nats: message is nil")
	}
	if p.nc == nil || !p.nc.IsConnected() {
		return fmt.Errorf("nats: connection is closed")
	}

	subject := p.cfg.Subject
	if msg.Topic != "" {
		subject = msg.Topic
	}

	nMsg := &natsgo.Msg{
		Subject: subject,
		Data:    msg.Payload,
		Header:  make(natsgo.Header),
	}
	for k, v := range msg.Headers {
		nMsg.Header.Set(k, v)
	}
	if msg.ID != "" {
		nMsg.Header.Set("Nats-Msg-Id", msg.ID)
	}

	var err error
	if p.js != nil {
		_, err = p.js.PublishMsg(nMsg, natsgo.Context(ctx))
	} else {
		err = p.nc.PublishMsg(nMsg)
	}
	if err != nil {
		return fmt.Errorf("nats: publish failed: %w", err)
	}
	return nil
}

// Close closes the NATS connection.
func (p *Producer) Close() error {
	if p.nc == nil {
		return nil
	}
	p.nc.Close()
	p.nc = nil
	p.js = nil
	return nil
}

func reconnectWait(d time.Duration) time.Duration {
	if d <= 0 {
		return 2 * time.Second
	}
	return d
}

func connectTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return 5 * time.Second
	}
	return d
}
