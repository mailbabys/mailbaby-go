// Package kafka implements a MailBaby queue publisher over Apache Kafka.
// The publishing behavior mirrors the MailBaby server's kafka driver: the
// message payload is sent as the record value, headers are attached as
// Kafka record headers (plus X-Message-ID), and Message.Key is used as the
// record key for partition sharding.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/mailbabys/mailbaby-go/mq"
)

// TLSConfig mirrors the server's kafka TLS settings.
type TLSConfig struct {
	Enable             bool
	InsecureSkipVerify bool
	CAFile             string
	CertFile           string
	KeyFile            string
}

// SASLConfig mirrors the server's kafka SASL settings.
type SASLConfig struct {
	Enable    bool
	Mechanism string // "PLAIN", "SCRAM-SHA-256" or "SCRAM-SHA-512"
	User      string
	Password  string
}

// Config configures the Kafka producer. Field semantics match the MailBaby
// server's queue.kafka configuration.
type Config struct {
	Brokers  []string
	Topic    string
	ClientID string
	TLS      TLSConfig
	SASL     SASLConfig
	// WriteTimeout is the writer timeout. Defaults to 10 seconds.
	WriteTimeout time.Duration
	// RequiredAcks controls durability. Defaults to kafka.RequireAll.
	RequiredAcks kafkago.RequiredAcks
}

// Producer publishes email jobs to a Kafka topic in the MailBaby wire
// format.
type Producer struct {
	cfg    Config
	writer *kafkago.Writer
}

// New creates a Kafka Producer with the given configuration. No network I/O
// is performed until the first Publish.
func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
	}

	tlsConfig, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	mechanism, err := buildSASLMechanism(cfg.SASL)
	if err != nil {
		return nil, err
	}

	dialer := &kafkago.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
		ClientID:      cfg.ClientID,
	}

	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	requiredAcks := cfg.RequiredAcks
	if requiredAcks == 0 {
		requiredAcks = kafkago.RequireAll
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafkago.LeastBytes{},
		Transport:    &kafkago.Transport{TLS: tlsConfig, SASL: mechanism, ClientID: cfg.ClientID, Dial: dialer.DialFunc},
		WriteTimeout: writeTimeout,
		ReadTimeout:  writeTimeout,
		RequiredAcks: requiredAcks,
	}

	return &Producer{cfg: cfg, writer: writer}, nil
}

// Publish sends a single message to the configured topic.
func (p *Producer) Publish(ctx context.Context, msg *mq.Message) error {
	if msg == nil {
		return fmt.Errorf("kafka: message is nil")
	}
	if p.writer == nil {
		return fmt.Errorf("kafka: producer is closed")
	}

	kMsg := newKafkaMessage(p.cfg.Topic, msg)
	if err := p.writer.WriteMessages(ctx, kMsg); err != nil {
		return fmt.Errorf("kafka: publish failed: %w", err)
	}
	return nil
}

// newKafkaMessage builds a kafka-go Message from a MailBaby queue message,
// mirroring the server's field mapping (topic override, record key, headers
// plus X-Message-ID).
func newKafkaMessage(defaultTopic string, msg *mq.Message) kafkago.Message {
	topic := defaultTopic
	if msg.Topic != "" {
		topic = msg.Topic
	}

	var headers []kafkago.Header
	for k, v := range msg.Headers {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	if msg.ID != "" {
		headers = append(headers, kafkago.Header{Key: "X-Message-ID", Value: []byte(msg.ID)})
	}

	return kafkago.Message{
		Topic:   topic,
		Key:     []byte(msg.Key),
		Value:   msg.Payload,
		Headers: headers,
		Time:    msg.Timestamp,
	}
}

// Close closes the underlying writer, flushing any buffered messages.
func (p *Producer) Close() error {
	if p.writer == nil {
		return nil
	}
	err := p.writer.Close()
	p.writer = nil
	return err
}

func buildTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if !cfg.Enable {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // explicit user opt-in
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("kafka: failed to parse CA certificates")
		}
		tlsCfg.RootCAs = pool
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func buildSASLMechanism(cfg SASLConfig) (sasl.Mechanism, error) {
	if !cfg.Enable {
		return nil, nil
	}

	switch strings.ToUpper(strings.TrimSpace(cfg.Mechanism)) {
	case "PLAIN", "":
		return plain.Mechanism{Username: cfg.User, Password: cfg.Password}, nil
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, cfg.User, cfg.Password)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, cfg.User, cfg.Password)
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %q", cfg.Mechanism)
	}
}
