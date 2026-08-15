package mq

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	mailbaby "github.com/mailbabys/mailbaby-go"
)

type fakePublisher struct {
	published []*Message
	err       error
}

func (f *fakePublisher) Publish(ctx context.Context, msg *Message) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, msg)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func TestNewEmailMessage(t *testing.T) {
	email := mailbaby.NewEmail().
		SetAccount("default").
		SetFrom("noreply@example.com").
		AddTo("alice@example.com").
		SetSubject("Hi").
		SetTextBody("hello")

	msg, err := NewEmailMessage(email)
	if err != nil {
		t.Fatalf("NewEmailMessage: %v", err)
	}
	if msg.ID == "" {
		t.Error("expected generated message ID")
	}
	if msg.GetHeader("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type header, got %q", msg.GetHeader("Content-Type"))
	}

	var payload mailbaby.Email
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		t.Fatalf("payload is not valid email JSON: %v", err)
	}
	if payload.Subject != "Hi" || len(payload.To) != 1 || payload.Account != "default" {
		t.Errorf("payload mismatch: %+v", payload)
	}

	msg2, err := NewEmailMessage(mailbaby.NewEmail().AddTo("b@c.com").SetSubject("x"))
	if err != nil {
		t.Fatalf("NewEmailMessage: %v", err)
	}
	if msg2.ID == msg.ID {
		t.Error("expected distinct IDs")
	}
}

func TestNewEmailMessageValidation(t *testing.T) {
	if _, err := NewEmailMessage(&mailbaby.Email{}); !errors.Is(err, mailbaby.ErrNoRecipients) {
		t.Errorf("expected ErrNoRecipients, got %v", err)
	}
}

func TestMessageBindJSON(t *testing.T) {
	email := mailbaby.NewEmail().AddTo("a@b.com").SetSubject("S")
	msg, err := NewEmailMessage(email)
	if err != nil {
		t.Fatalf("NewEmailMessage: %v", err)
	}

	var round mailbaby.Email
	if err := msg.BindJSON(&round); err != nil {
		t.Fatalf("BindJSON: %v", err)
	}
	if round.Subject != "S" {
		t.Errorf("unexpected subject %q", round.Subject)
	}

	if err := (&Message{}).BindJSON(&round); err == nil {
		t.Error("expected error for empty payload")
	}
}

func TestPublishEmail(t *testing.T) {
	pub := &fakePublisher{}
	email := mailbaby.NewEmail().AddTo("a@b.com").SetSubject("S")

	msg, err := PublishEmail(context.Background(), pub, "mail_topic", email)
	if err != nil {
		t.Fatalf("PublishEmail: %v", err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(pub.published))
	}
	if pub.published[0].Topic != "mail_topic" {
		t.Errorf("unexpected topic %q", pub.published[0].Topic)
	}
	if msg != pub.published[0] {
		t.Error("returned message should be the published one")
	}
}

func TestPublishEmailNilPublisher(t *testing.T) {
	if _, err := PublishEmail(context.Background(), nil, "t", mailbaby.NewEmail().AddTo("a@b.com").SetSubject("S")); err == nil {
		t.Fatal("expected error for nil publisher")
	}
}

func TestMessageHeaders(t *testing.T) {
	msg := NewMessage([]byte("x"))
	msg.SetHeader("K", "v")
	if msg.GetHeader("K") != "v" {
		t.Errorf("unexpected header value %q", msg.GetHeader("K"))
	}
	if msg.GetHeader("Missing") != "" {
		t.Error("expected empty header")
	}
}
