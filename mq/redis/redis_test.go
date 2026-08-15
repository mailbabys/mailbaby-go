package redis

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mailbabys/mailbaby-go/mq"
)

func startMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	m := miniredis.RunT(t)
	return m
}

func newTestProducer(t *testing.T, m *miniredis.Miniredis, mode string) *Producer {
	t.Helper()
	p, err := New(Config{Addr: m.Addr(), Key: "mail_queue", Mode: mode})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := New(Config{Addr: "x:1"}); err == nil {
		t.Fatal("expected error for missing key")
	}
	if _, err := New(Config{Addr: "x:1", Key: "k", Mode: "bogus"}); err == nil {
		t.Fatal("expected error for unsupported mode")
	}
}

func TestNewConnectFailure(t *testing.T) {
	if _, err := New(Config{Addr: "127.0.0.1:1", Key: "k"}); err == nil {
		t.Fatal("expected connection error")
	}
}

func TestPublishStream(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeStream)

	msg := mq.NewMessageWithID("msg-1", []byte(`{"subject":"stream"}`))
	msg.SetHeader("X-Custom", "v1")
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	defer client.Close()

	entries, err := client.XRange(context.Background(), "mail_queue", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry, got %d", len(entries))
	}
	if entries[0].Values["id"] != "msg-1" {
		t.Errorf("unexpected stream id %q", entries[0].Values["id"])
	}

	var env envelope
	dataStr, ok := entries[0].Values["data"].(string)
	if !ok {
		t.Fatalf("stream data field missing or not a string: %+v", entries[0].Values)
	}
	if err := json.Unmarshal([]byte(dataStr), &env); err != nil {
		t.Fatalf("stream data is not a valid envelope: %v", err)
	}
	if env.ID != "msg-1" || env.Topic != "mail_queue" {
		t.Errorf("unexpected envelope: %+v", env)
	}
	if string(env.Payload) != `{"subject":"stream"}` {
		t.Errorf("unexpected payload %q", env.Payload)
	}
	if env.Headers["X-Custom"] != "v1" {
		t.Errorf("unexpected headers: %+v", env.Headers)
	}
	if env.Attempts != 1 || env.Timestamp.IsZero() {
		t.Errorf("unexpected envelope metadata: %+v", env)
	}
}

func TestPublishList(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeList)

	if err := p.Publish(context.Background(), mq.NewMessageWithID("list-1", []byte(`{"subject":"list"}`))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	items, err := m.List("mail_queue")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 list item, got %d", len(items))
	}
	var env envelope
	if err := json.Unmarshal([]byte(items[0]), &env); err != nil {
		t.Fatalf("list item is not a valid envelope: %v", err)
	}
	if env.ID != "list-1" || string(env.Payload) != `{"subject":"list"}` {
		t.Errorf("unexpected envelope: %+v", env)
	}
}

func TestPublishPubSub(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModePubSub)

	client := goredis.NewClient(&goredis.Options{Addr: m.Addr()})
	defer client.Close()

	sub := client.Subscribe(context.Background(), "mail_queue")
	defer sub.Close()
	// consume any prior messages before the subscribe is registered
	_, _ = sub.ReceiveTimeout(context.Background(), time.Millisecond)

	if err := p.Publish(context.Background(), mq.NewMessageWithID("ps-1", []byte(`{"subject":"pubsub"}`))); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, err := sub.ReceiveTimeout(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("ReceiveTimeout: %v", err)
	}
	payload, ok := msg.(*goredis.Message)
	if !ok {
		t.Fatalf("unexpected message type %T", msg)
	}
	var env envelope
	if err := json.Unmarshal([]byte(payload.Payload), &env); err != nil {
		t.Fatalf("pubsub payload is not a valid envelope: %v", err)
	}
	if env.ID != "ps-1" || string(env.Payload) != `{"subject":"pubsub"}` {
		t.Errorf("unexpected envelope: %+v", env)
	}
}

func TestPublishTopicOverride(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeList)

	msg := mq.NewMessageWithID("t-1", []byte("x"))
	msg.Topic = "other_queue"
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if lenMust(t, m, "other_queue") != 1 {
		t.Errorf("expected message in overridden queue")
	}
	if lenMust(t, m, "mail_queue") != 0 {
		t.Errorf("expected no message in default queue")
	}
}

func lenMust(t *testing.T, m *miniredis.Miniredis, key string) int {
	t.Helper()
	if !m.Exists(key) {
		return 0
	}
	items, err := m.List(key)
	if err != nil {
		t.Fatalf("List(%q): %v", key, err)
	}
	return len(items)
}

func TestPublishDelay(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeList)

	msg := mq.NewMessageWithID("d-1", []byte("x"))
	msg.Delay = 100 * time.Millisecond
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if got := lenMust(t, m, "mail_queue"); got != 0 {
		t.Errorf("message should be delayed, got %d items", got)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := lenMust(t, m, "mail_queue"); got == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("delayed message never appeared")
}

func TestPublishNilMessage(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeList)
	if err := p.Publish(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil message")
	}
}

func TestPublishAfterClose(t *testing.T) {
	m := startMiniredis(t)
	p := newTestProducer(t, m, ModeList)
	_ = p.Close()
	if err := p.Publish(context.Background(), mq.NewMessage([]byte("x"))); err == nil {
		t.Fatal("expected error after close")
	}
}
