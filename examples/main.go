// Command example demonstrates the three MailBaby client channels: HTTP
// REST, gRPC and direct message-queue publishing.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	mailbaby "github.com/mailbabys/mailbaby-go"
	"github.com/mailbabys/mailbaby-go/mq"
	kafkamq "github.com/mailbabys/mailbaby-go/mq/kafka"
	natsmq "github.com/mailbabys/mailbaby-go/mq/nats"
	rabbitmq "github.com/mailbabys/mailbaby-go/mq/rabbitmq"
	redismq "github.com/mailbabys/mailbaby-go/mq/redis"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---------------------------------------------------------------------
	// 1. REST API
	// ---------------------------------------------------------------------
	client, err := mailbaby.New("http://localhost:8080", mailbaby.WithAPIKey("your_secret_key"))
	if err != nil {
		log.Fatalf("new rest client: %v", err)
	}

	email := mailbaby.NewEmail().
		SetAccount("default").
		SetFrom("noreply@example.com", "MailBaby System").
		SetReplyTo("support@example.com").
		AddTo("alice@example.com").
		AddCc("manager@example.com").
		SetSubject("Order Confirmation #10024").
		SetTextBody("Thank you for your order!").
		SetHTMLBody("<h2>Order Confirmed</h2>").
		SetHeader("X-Priority", "1").
		AddTag("order").
		AddTag("receipt").
		Attach("invoice.pdf", []byte("%PDF-1.4 fake"), "application/pdf")

	resp, err := client.Send(ctx, email) // synchronous
	if err != nil {
		log.Fatalf("send: %v", err)
	}
	fmt.Printf("REST send: id=%s status=%s\n", resp.ID, resp.Status)

	asyncResp, err := client.SendAsync(ctx, mailbaby.NewEmail().
		AddTo("bob@example.com").
		SetSubject("Welcome!").
		SetHTMLBody("<h1>Welcome aboard!</h1>"))
	if err != nil {
		log.Fatalf("send async: %v", err)
	}
	fmt.Printf("REST async: id=%s status=%s\n", asyncResp.ID, asyncResp.Status)

	batch, err := client.SendBatch(ctx, []*mailbaby.Email{
		mailbaby.NewEmail().AddTo("u1@example.com").SetSubject("Statement 1"),
		mailbaby.NewEmail().AddTo("u2@example.com").SetSubject("Statement 2"),
	}, true)
	if err != nil {
		log.Fatalf("send batch: %v", err)
	}
	fmt.Printf("REST batch: total=%d succeeded=%d failed=%d\n", batch.Total, batch.Succeeded, batch.Failed)

	// ---------------------------------------------------------------------
	// 2. gRPC
	// ---------------------------------------------------------------------
	grpcClient, err := mailbaby.Dial(ctx, "localhost:8081", mailbaby.WithGRPCAuth("your_secret_key"))
	if err != nil {
		log.Fatalf("dial grpc: %v", err)
	}
	defer grpcClient.Close()

	grpcResp, err := grpcClient.Send(ctx, mailbaby.NewEmail().
		AddTo("dev@example.com").
		SetSubject("gRPC Email").
		SetTextBody("Sent via MailService.Send"))
	if err != nil {
		log.Fatalf("grpc send: %v", err)
	}
	fmt.Printf("gRPC send: id=%s status=%s\n", grpcResp.ID, grpcResp.Status)

	health, err := grpcClient.HealthCheck(ctx, "")
	if err != nil {
		log.Fatalf("grpc health: %v", err)
	}
	fmt.Printf("gRPC health: %v details=%v\n", health.Status, health.Details)

	// ---------------------------------------------------------------------
	// 3. Message-queue publishing (RabbitMQ / Kafka / Redis / NATS)
	// ---------------------------------------------------------------------
	if err := publishToRabbitMQ(ctx); err != nil {
		log.Printf("rabbitmq: %v", err)
	}
	if err := publishToKafka(ctx); err != nil {
		log.Printf("kafka: %v", err)
	}
	if err := publishToRedis(ctx); err != nil {
		log.Printf("redis: %v", err)
	}
	if err := publishToNATS(ctx); err != nil {
		log.Printf("nats: %v", err)
	}
	_ = os.Stdout
}

func publishToRabbitMQ(ctx context.Context) error {
	p, err := rabbitmq.New(rabbitmq.Config{
		URL:   "amqp://guest:guest@localhost:5672/",
		Queue: "mail_queue",
	})
	if err != nil {
		return err
	}
	defer p.Close()

	msg, err := mq.PublishEmail(ctx, p, "mail_queue", mailbaby.NewEmail().
		AddTo("oncall@example.com").
		SetSubject("[CRITICAL] High CPU Load").
		SetTextBody("CPU usage exceeded 95%."))
	if err != nil {
		return err
	}
	fmt.Printf("rabbitmq published: id=%s\n", msg.ID)
	return nil
}

func publishToKafka(ctx context.Context) error {
	p, err := kafkamq.New(kafkamq.Config{
		Brokers: []string{"localhost:9092"},
		Topic:   "mail_queue",
	})
	if err != nil {
		return err
	}
	defer p.Close()

	m, err := mq.NewEmailMessage(mailbaby.NewEmail().
		AddTo("kafka@example.com").
		SetSubject("From Kafka"))
	if err != nil {
		return err
	}
	m.Key = "partition-shard-1" // optional partition key
	if err := p.Publish(ctx, m); err != nil {
		return err
	}
	fmt.Printf("kafka published: id=%s\n", m.ID)
	return nil
}

func publishToRedis(ctx context.Context) error {
	p, err := redismq.New(redismq.Config{
		Addr: "localhost:6379",
		Key:  "mail_queue",
		Mode: redismq.ModeStream,
	})
	if err != nil {
		return err
	}
	defer p.Close()

	msg, err := mq.PublishEmail(ctx, p, "mail_queue", mailbaby.NewEmail().
		AddTo("redis@example.com").
		SetSubject("From Redis Stream"))
	if err != nil {
		return err
	}
	fmt.Printf("redis published: id=%s\n", msg.ID)
	return nil
}

func publishToNATS(ctx context.Context) error {
	p, err := natsmq.New(natsmq.Config{
		URL:     "nats://localhost:4222",
		Subject: "mail_queue",
	})
	if err != nil {
		return err
	}
	defer p.Close()

	msg, err := mq.PublishEmail(ctx, p, "mail_queue", mailbaby.NewEmail().
		AddTo("nats@example.com").
		SetSubject("From NATS"))
	if err != nil {
		return err
	}
	fmt.Printf("nats published: id=%s\n", msg.ID)
	return nil
}
