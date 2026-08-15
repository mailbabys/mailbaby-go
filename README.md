<div align="center">

# 📬 MailBaby Go Client

**Official Go client for the [MailBaby](https://github.com/mailbabys/mailbaby) email delivery microservice**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![Go Report Card](https://img.shields.io/badge/Go%20Report-A%2B-brightgreen.svg)]()

</div>

`mailbaby-go` is a batteries-included client for MailBaby covering all three
dispatch channels:

- **HTTP REST API** — synchronous / asynchronous single send, batch send, health probes
- **gRPC `MailService`** — `Send`, `SendBatch`, `Ping`, `HealthCheck`
- **Message-queue publishing** — RabbitMQ, Kafka, Redis (stream/list/pubsub), NATS
  (core / JetStream), with payloads in the exact wire format the MailBaby
  server consumes

## 📦 Installation

```bash
go get github.com/mailbabys/mailbaby-go
```

## 🚀 Quick Start

### 1. HTTP REST

```go
package main

import (
	"context"
	"fmt"

	mailbaby "github.com/mailbabys/mailbaby-go"
)

func main() {
	client, err := mailbaby.New("http://localhost:8080",
		mailbaby.WithAPIKey("your_secret_key"))
	if err != nil {
		panic(err)
	}

	email := mailbaby.NewEmail().
		SetAccount("default").
		SetFrom("noreply@example.com", "MailBaby System").
		AddTo("alice@example.com").
		SetSubject("Order Confirmation #10024").
		SetHTMLBody("<h2>Order Confirmed</h2>").
		Attach("invoice.pdf", pdfBytes, "application/pdf")

	// synchronous: blocks until SMTP acknowledges
	resp, err := client.Send(context.Background(), email)
	fmt.Printf("id=%s status=%s\n", resp.ID, resp.Status)

	// asynchronous: enqueues and returns immediately (status "queued")
	async, err := client.SendAsync(context.Background(), email)

	// batch: per-item results in BatchResponse.Results
	batch, err := client.SendBatch(context.Background(), []*mailbaby.Email{email}, false)

	// health probes
	live, _ := client.Livez(context.Background())
	ready, _ := client.Readyz(context.Background())
	text, _ := client.Healthz(context.Background())
}
```

**Error handling**: non-2xx responses are returned as `*mailbaby.APIError`,
classifiable with `errors.Is`:

```go
var apiErr *mailbaby.APIError
if errors.As(err, &apiErr) {
	fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.Details)
}
switch {
case errors.Is(err, mailbaby.ErrUnauthorized):
	// 401 — bad or missing token
case errors.Is(err, mailbaby.ErrValidation):
	// 400 — payload rejected
case errors.Is(err, mailbaby.ErrDeliveryFailed):
	// 500 — SMTP delivery failed
}
```

### 2. gRPC

```go
import (
	"google.golang.org/grpc/credentials/insecure"

	mailbaby "github.com/mailbabys/mailbaby-go"
)

func sendGRPC() error {
	conn, err := grpc.NewClient("localhost:8081",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	client := mailbaby.NewGRPCClient(conn, mailbaby.WithGRPCAuth("your_secret_key"))
	defer client.Close()

	resp, err := client.Send(context.Background(),
		mailbaby.NewEmail().AddTo("dev@example.com").SetSubject("gRPC Email"))
	if err != nil {
		return err
	}
	fmt.Println(resp.ID, resp.Status)

	pong, err := client.Ping(context.Background(), "hello")
	health, err := client.HealthCheck(context.Background(), "")
	return nil
}
```

### 3. Message-queue publishing

Publishing to any supported broker produces payloads an **unmodified MailBaby
server** consumes — just match the topic name in your server config
(e.g. `queue.kafka.topic`, `queue.rabbitmq.queue`, `queue.redis.key`,
`queue.nats.subject`).

```go
import (
	"github.com/mailbabys/mailbaby-go/mq"
	rabbitmq "github.com/mailbabys/mailbaby-go/mq/rabbitmq"
)

func publish() error {
	p, err := rabbitmq.New(rabbitmq.Config{
		URL:   "amqp://guest:guest@localhost:5672/",
		Queue: "mail_queue",
	})
	if err != nil {
		return err
	}
	defer p.Close()

	msg, err := mq.PublishEmail(context.Background(), p, "mail_queue",
		mailbaby.NewEmail().
			AddTo("oncall@example.com").
			SetSubject("[CRITICAL] High CPU Load").
			SetTextBody("CPU usage exceeded 95%."))
	if err != nil {
		return err
	}
	fmt.Println("published", msg.ID)
	return nil
}
```

| Driver | Package | Config highlights |
|---|---|---|
| RabbitMQ | `mq/rabbitmq` | URL, Queue, optional Exchange/RoutingKey/TLS-free topology, AMQP headers, MessageId |
| Kafka | `mq/kafka` | Brokers, Topic, TLS + SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512), partition Key, X-Message-ID header |
| Redis | `mq/redis` | Addr, Key, Mode (`stream` \| `list` \| `pubsub`), MaxLen, delayed publish, server-compatible envelope |
| NATS | `mq/nats` | URL, Subject, JetStream, credentials, `Nats-Msg-Id` header |

Each driver implements the `mq.Publisher` interface:

```go
type Publisher interface {
	Publish(ctx context.Context, msg *mq.Message) error
	Close() error
}
```

Use `mq.NewEmailMessage(email)` to build a `*mq.Message` from a validated
`*mailbaby.Email` (payload = MailBaby wire JSON, `Content-Type:
application/json` header, message ID propagated), or `mq.NewMessage(payload)`
for custom payloads.

## 🧪 Testing

```bash
make vet test      # unit tests (no external brokers required)
make test          # same
```

Unit tests use in-process servers (`httptest`, gRPC `bufconn`, `miniredis`,
embedded `nats-server`). Broker integration tests for RabbitMQ are behind the
`integration` build tag:

```bash
go test -tags integration ./mq/rabbitmq/ -run Integration -v
```

## 🔧 Regenerating gRPC stubs

The `pb/` package is generated from `pb/mailbaby.proto` (a vendored copy of
the server's proto definition):

```bash
make proto   # requires protoc + protoc-gen-go + protoc-gen-go-grpc
```

## 📄 License

Apache-2.0 — see [LICENSE](LICENSE).
