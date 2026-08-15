package nats

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"

	"github.com/mailbabys/mailbaby-go/mq"
)

func runNATSServer(t *testing.T, jetStream bool) string {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: jetStream,
	}
	if jetStream {
		opts.StoreDir = t.TempDir()
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("nats server did not become ready")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for empty config")
	}
	if _, err := New(Config{URL: "nats://127.0.0.1:1"}); err == nil {
		t.Fatal("expected error for missing subject")
	}
}

func TestPublishCore(t *testing.T) {
	url := runNATSServer(t, false)

	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("mail_queue")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	p, err := New(Config{URL: url, Subject: "mail_queue"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	msg := mq.NewMessageWithID("core-1", []byte(`{"subject":"core"}`))
	msg.SetHeader("X-Custom", "v1")
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	recv, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if string(recv.Data) != `{"subject":"core"}` {
		t.Errorf("unexpected data %q", recv.Data)
	}
	if recv.Header.Get("X-Custom") != "v1" {
		t.Errorf("unexpected headers: %+v", recv.Header)
	}
	if recv.Header.Get("Nats-Msg-Id") != "core-1" {
		t.Errorf("unexpected Nats-Msg-Id %q", recv.Header.Get("Nats-Msg-Id"))
	}
}

func TestPublishTopicOverride(t *testing.T) {
	url := runNATSServer(t, false)

	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("other_subject")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	p, err := New(Config{URL: url, Subject: "mail_queue"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	msg := mq.NewMessageWithID("t-1", []byte("x"))
	msg.Topic = "other_subject"
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if _, err := sub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("NextMsg on overridden subject: %v", err)
	}
}

func TestPublishJetStream(t *testing.T) {
	url := runNATSServer(t, true)

	nc, err := natsgo.Connect(url)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if _, err := js.AddStream(&natsgo.StreamConfig{
		Name:     "MAILS",
		Subjects: []string{"mail_queue"},
	}); err != nil {
		t.Fatalf("AddStream: %v", err)
	}

	p, err := New(Config{URL: url, Subject: "mail_queue", JetStream: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	msg := mq.NewMessageWithID("js-1", []byte(`{"subject":"jetstream"}`))
	if err := p.Publish(context.Background(), msg); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	sub, err := js.SubscribeSync("mail_queue", natsgo.BindStream("MAILS"))
	if err != nil {
		t.Fatalf("BindStream subscribe: %v", err)
	}
	recv, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if string(recv.Data) != `{"subject":"jetstream"}` {
		t.Errorf("unexpected data %q", recv.Data)
	}
	if recv.Header.Get("Nats-Msg-Id") != "js-1" {
		t.Errorf("unexpected Nats-Msg-Id %q", recv.Header.Get("Nats-Msg-Id"))
	}
}

func TestPublishNilMessage(t *testing.T) {
	url := runNATSServer(t, false)
	p, err := New(Config{URL: url, Subject: "mail_queue"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer p.Close()

	if err := p.Publish(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil message")
	}
}
