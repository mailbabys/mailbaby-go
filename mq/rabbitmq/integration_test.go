//go:build integration

// Integration tests against a live RabbitMQ broker.
// Run with: go test -tags integration ./mq/rabbitmq/ -run Integration
// Requires a broker reachable via RABBITMQ_URL (default amqp://localhost:5672).
package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mailbabys/mailbaby-go/mq"
)

func TestIntegrationPublish(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	p, err := New(Config{URL: url, Queue: "mail_queue_integration"})
	if err != nil {
		t.Skipf("broker unavailable: %v", err)
	}
	defer p.Close()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	deliveries, err := ch.Consume("mail_queue_integration", "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	msg := mq.NewMessageWithID("it-1", []byte(`{"subject":"integration"}`))
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case d := <-deliveries:
		if string(d.Body) != `{"subject":"integration"}` {
			t.Errorf("unexpected body %q", d.Body)
		}
		if d.MessageId != "it-1" {
			t.Errorf("unexpected MessageId %q", d.MessageId)
		}
		if err := d.Ack(false); err != nil {
			t.Errorf("Ack: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}
