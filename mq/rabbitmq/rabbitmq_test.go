package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/mailbabys/mailbaby-go/mq"
)

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := New(Config{URL: "amqp://guest:guest@localhost:5672/"}); err == nil {
		t.Fatal("expected error for missing queue")
	}
	if _, err := New(Config{URL: "amqp://guest:guest@127.0.0.1:1/", Queue: "q", DialTimeout: time.Second}); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestPublishNilMessage(t *testing.T) {
	p := &Producer{}
	if err := p.Publish(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil message")
	}
}

func TestPublishClosed(t *testing.T) {
	p := &Producer{}
	if err := p.Publish(context.Background(), mq.NewMessage([]byte("x"))); err == nil {
		t.Fatal("expected error for closed connection")
	}
}
