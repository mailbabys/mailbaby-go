// Package rabbitmq implements a MailBaby queue publisher over AMQP 0-9-1.
// The publishing behavior mirrors the MailBaby server's rabbitmq driver:
// the message payload is delivered as the AMQP body, headers are attached
// as AMQP table entries, and the message ID is propagated in the
// MessageId field.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mailbabys/mailbaby-go/mq"
)

// Config configures the RabbitMQ producer. Field semantics match the
// MailBaby server's queue.rabbitmq configuration.
type Config struct {
	// URL is the AMQP connection URL (e.g. amqp://guest:guest@localhost:5672/).
	URL string
	// Queue is the destination queue name.
	Queue string
	// Exchange is an optional topic exchange to publish through.
	Exchange string
	// RoutingKey is the routing key used for exchange publishes. Defaults to
	// the message Topic when empty.
	RoutingKey string
	// Durable marks the exchange and queue as durable.
	Durable bool
	// AutoDelete marks the exchange and queue as auto-delete.
	AutoDelete bool
	// Exclusive marks the queue as exclusive.
	Exclusive bool
	// NoWait disables server-side wait for declare confirmations.
	NoWait bool
	// Args contains optional queue arguments (e.g. x-dead-letter-exchange).
	Args map[string]any
	// DialTimeout is the connection timeout. Defaults to 10 seconds.
	DialTimeout time.Duration
}

// Producer publishes email jobs to a RabbitMQ queue in the MailBaby wire
// format.
type Producer struct {
	cfg  Config
	conn *amqp.Connection
	ch   *amqp.Channel
	mu   sync.Mutex
}

// New creates a Producer, establishing the connection and declaring the
// exchange/queue topology (mirroring the server's setup).
func New(cfg Config) (*Producer, error) {
	if cfg.URL == "" {
		return nil, errors.New("rabbitmq: url is required")
	}
	if cfg.Queue == "" {
		return nil, errors.New("rabbitmq: queue is required")
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	p := &Producer{cfg: cfg}
	if err := p.connect(timeout); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Producer) connect(timeout time.Duration) error {
	conn, err := amqp.DialConfig(p.cfg.URL, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(timeout),
	})
	if err != nil {
		return fmt.Errorf("rabbitmq: dial failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("rabbitmq: open channel failed: %w", err)
	}

	p.mu.Lock()
	p.conn = conn
	p.ch = ch
	p.mu.Unlock()

	if err := p.setupTopology(); err != nil {
		_ = p.Close()
		return err
	}
	return nil
}

func (p *Producer) setupTopology() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var args amqp.Table
	if len(p.cfg.Args) > 0 {
		args = amqp.Table(p.cfg.Args)
	}

	if p.cfg.Exchange != "" {
		if err := p.ch.ExchangeDeclare(
			p.cfg.Exchange,
			"topic",
			p.cfg.Durable,
			p.cfg.AutoDelete,
			false,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("rabbitmq: exchange declare failed: %w", err)
		}
	}

	if p.cfg.Queue != "" {
		if _, err := p.ch.QueueDeclare(
			p.cfg.Queue,
			p.cfg.Durable,
			p.cfg.AutoDelete,
			p.cfg.Exclusive,
			p.cfg.NoWait,
			args,
		); err != nil {
			return fmt.Errorf("rabbitmq: queue declare failed: %w", err)
		}

		if p.cfg.Exchange != "" {
			routingKey := p.cfg.RoutingKey
			if routingKey == "" {
				routingKey = "#"
			}
			if err := p.ch.QueueBind(p.cfg.Queue, routingKey, p.cfg.Exchange, false, nil); err != nil {
				return fmt.Errorf("rabbitmq: queue bind failed: %w", err)
			}
		}
	}
	return nil
}

// Publish sends a single message to the configured queue.
func (p *Producer) Publish(ctx context.Context, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("rabbitmq: message is nil")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.conn == nil || p.conn.IsClosed() {
		return fmt.Errorf("rabbitmq: connection is closed")
	}

	exchange := p.cfg.Exchange
	routingKey := p.cfg.RoutingKey
	if msg.Topic != "" {
		routingKey = msg.Topic
	}

	table := make(amqp.Table)
	for k, v := range msg.Headers {
		table[k] = v
	}

	publishing := amqp.Publishing{
		Headers:      table,
		ContentType:  "application/octet-stream",
		DeliveryMode: amqp.Persistent,
		MessageId:    msg.ID,
		Timestamp:    msg.Timestamp,
		Body:         msg.Payload,
	}
	if ct, ok := table["Content-Type"].(string); ok && ct != "" {
		publishing.ContentType = ct
	}

	if err := p.ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		publishing,
	); err != nil {
		return fmt.Errorf("rabbitmq: publish failed: %w", err)
	}
	return nil
}

// Close closes the channel and connection.
func (p *Producer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	if p.ch != nil {
		if err := p.ch.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
