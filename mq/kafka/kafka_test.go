package kafka

import (
	"context"
	"testing"

	"github.com/mailbabys/mailbaby-go/mq"
)

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := New(Config{Brokers: []string{"localhost:9092"}}); err == nil {
		t.Fatal("expected error for missing topic")
	}
	if _, err := New(Config{Brokers: []string{"localhost:9092"}, Topic: "t", SASL: SASLConfig{Enable: true, Mechanism: "BAD"}}); err == nil {
		t.Fatal("expected error for unsupported SASL mechanism")
	}
}

func TestNewKafkaMessage(t *testing.T) {
	msg := mq.NewMessageWithID("msg-1", []byte(`{"subject":"S"}`))
	msg.Topic = "override_topic"
	msg.Key = "shard-42"
	msg.SetHeader("X-Custom", "v1")

	km := newKafkaMessage("default_topic", msg)
	if km.Topic != "override_topic" {
		t.Errorf("expected topic override, got %q", km.Topic)
	}
	if string(km.Key) != "shard-42" {
		t.Errorf("unexpected key %q", km.Key)
	}
	if string(km.Value) != `{"subject":"S"}` {
		t.Errorf("unexpected value %q", km.Value)
	}
	if km.Time.IsZero() {
		t.Error("expected timestamp")
	}

	found := map[string]string{}
	for _, h := range km.Headers {
		found[h.Key] = string(h.Value)
	}
	if found["X-Custom"] != "v1" || found["X-Message-ID"] != "msg-1" {
		t.Errorf("unexpected headers: %+v", found)
	}

	// default topic when Message.Topic is empty
	km2 := newKafkaMessage("default_topic", mq.NewMessageWithID("id2", nil))
	if km2.Topic != "default_topic" {
		t.Errorf("expected default topic, got %q", km2.Topic)
	}
}

func TestPublishNilMessage(t *testing.T) {
	p, err := New(Config{Brokers: []string{"localhost:9092"}, Topic: "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()
	if err := p.Publish(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil message")
	}
}

func TestPublishAfterClose(t *testing.T) {
	p, err := New(Config{Brokers: []string{"localhost:9092"}, Topic: "t"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_ = p.Close()
	if err := p.Publish(context.Background(), mq.NewMessage([]byte("x"))); err == nil {
		t.Fatal("expected error after close")
	}
	if err := p.Close(); err != nil {
		t.Errorf("double close should be safe, got %v", err)
	}
}
