package mailbaby

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mailbabys/mailbaby-go/pb"
)

// ServingStatus mirrors the gRPC HealthCheckResponse serving status values.
type ServingStatus int32

const (
	// ServingStatusUnknown indicates the service status is unknown.
	ServingStatusUnknown ServingStatus = iota
	// ServingStatusServing indicates the service is serving.
	ServingStatusServing
	// ServingStatusNotServing indicates the service is not serving.
	ServingStatusNotServing
	// ServingStatusServiceUnknown indicates the requested service is unknown.
	ServingStatusServiceUnknown
)

func (s ServingStatus) String() string {
	switch s {
	case ServingStatusServing:
		return "SERVING"
	case ServingStatusNotServing:
		return "NOT_SERVING"
	case ServingStatusServiceUnknown:
		return "SERVICE_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

// PingResponse is the response of the MailService.Ping RPC.
type PingResponse struct {
	Status    string
	Version   string
	Timestamp int64
}

// HealthCheckResponse is the response of the MailService.HealthCheck RPC.
type HealthCheckResponse struct {
	Status  ServingStatus
	Details map[string]string
}

// GRPCClient is a client for the mailbaby.v1.MailService gRPC service.
// The caller owns the underlying *grpc.ClientConn lifecycle.
type GRPCClient struct {
	conn       *grpc.ClientConn
	client     pb.MailServiceClient
	apiKey     string
	headerName string
}

// GRPCOption configures a GRPCClient.
type GRPCOption func(*GRPCClient)

// WithGRPCAuth sets the secret key sent with every RPC. The key is attached
// as an "authorization: Bearer <key>" metadata entry (matching the server's
// token extraction rules). A custom header name can be set with
// WithGRPCAuthHeaderName.
func WithGRPCAuth(key string) GRPCOption {
	return func(c *GRPCClient) {
		c.apiKey = strings.TrimSpace(key)
	}
}

// WithGRPCAuthHeaderName overrides the metadata key carrying the API key.
// The value is lowercased as required by gRPC metadata. Defaults to
// "authorization" with a "Bearer " prefix.
func WithGRPCAuthHeaderName(name string) GRPCOption {
	return func(c *GRPCClient) {
		if strings.TrimSpace(name) != "" {
			c.headerName = strings.ToLower(strings.TrimSpace(name))
		}
	}
}

// NewGRPCClient creates a GRPCClient over an existing connection.
func NewGRPCClient(conn *grpc.ClientConn, opts ...GRPCOption) *GRPCClient {
	c := &GRPCClient{conn: conn, client: pb.NewMailServiceClient(conn)}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Dial creates a gRPC connection and a GRPCClient for the given target.
// Callers should invoke Close on the returned GRPCClient when done.
func Dial(ctx context.Context, target string, opts ...GRPCOption) (*GRPCClient, error) {
	conn, err := grpc.NewClient(target, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`))
	if err != nil {
		return nil, fmt.Errorf("mailbaby: failed to dial %q: %w", target, err)
	}
	return NewGRPCClient(conn, opts...), nil
}

// Conn returns the underlying grpc.ClientConn.
func (c *GRPCClient) Conn() *grpc.ClientConn {
	return c.conn
}

// Close closes the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Send delivers a single email synchronously unless Email.Async is set.
func (c *GRPCClient) Send(ctx context.Context, email *Email) (*SendResponse, error) {
	if err := email.Validate(); err != nil {
		return nil, err
	}
	req := emailToProto(email)
	resp, err := c.client.Send(c.authContext(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: grpc send failed: %w", err)
	}
	return sendResponseFromProto(resp), nil
}

// SendAsync enqueues a single email for asynchronous delivery.
func (c *GRPCClient) SendAsync(ctx context.Context, email *Email) (*SendResponse, error) {
	if err := email.Validate(); err != nil {
		return nil, err
	}
	req := emailToProto(email)
	req.Async = true
	resp, err := c.client.Send(c.authContext(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: grpc send failed: %w", err)
	}
	return sendResponseFromProto(resp), nil
}

// SendBatch delivers multiple emails; per-item failures are reported in the
// returned BatchResponse rather than as an error.
func (c *GRPCClient) SendBatch(ctx context.Context, emails []*Email, async bool) (*BatchResponse, error) {
	if len(emails) == 0 {
		return nil, ErrEmptyBatch
	}
	for _, e := range emails {
		if err := e.Validate(); err != nil {
			return nil, err
		}
	}

	req := &pb.BatchSendMailRequest{Async: async}
	for _, e := range emails {
		req.Emails = append(req.Emails, emailToProto(e))
	}

	resp, err := c.client.SendBatch(c.authContext(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: grpc send batch failed: %w", err)
	}
	return batchResponseFromProto(resp), nil
}

// Ping checks service liveness.
func (c *GRPCClient) Ping(ctx context.Context, message string) (*PingResponse, error) {
	resp, err := c.client.Ping(c.authContext(ctx), &pb.PingRequest{Message: message})
	if err != nil {
		return nil, fmt.Errorf("mailbaby: grpc ping failed: %w", err)
	}
	return &PingResponse{
		Status:    resp.Status,
		Version:   resp.Version,
		Timestamp: resp.Timestamp,
	}, nil
}

// HealthCheck checks service readiness and dependency details.
func (c *GRPCClient) HealthCheck(ctx context.Context, service string) (*HealthCheckResponse, error) {
	resp, err := c.client.HealthCheck(c.authContext(ctx), &pb.HealthCheckRequest{Service: service})
	if err != nil {
		return nil, fmt.Errorf("mailbaby: grpc health check failed: %w", err)
	}
	return &HealthCheckResponse{
		Status:  ServingStatus(resp.Status),
		Details: resp.Details,
	}, nil
}

func (c *GRPCClient) authContext(ctx context.Context) context.Context {
	if c.apiKey == "" {
		return ctx
	}
	if c.headerName != "" {
		return metadata.AppendToOutgoingContext(ctx, c.headerName, c.apiKey)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.apiKey)
}

// GRPCErrorCode extracts the gRPC status code from an error, if available.
func GRPCErrorCode(err error) (codes.Code, bool) {
	s, ok := status.FromError(err)
	if !ok || s == nil {
		return codes.OK, false
	}
	return s.Code(), true
}

func emailToProto(e *Email) *pb.SendMailRequest {
	req := &pb.SendMailRequest{
		Id:       e.ID,
		Account:  e.Account,
		From:     e.From,
		FromName: e.FromName,
		ReplyTo:  e.ReplyTo,
		To:       e.To,
		Cc:       e.Cc,
		Bcc:      e.Bcc,
		Subject:  e.Subject,
		TextBody: e.TextBody,
		HtmlBody: e.HTMLBody,
		Headers:  e.Headers,
		Tags:     e.Tags,
		Metadata: e.Metadata,
		Async:    e.Async,
	}
	for _, att := range e.Attachments {
		if att != nil {
			req.Attachments = append(req.Attachments, &pb.Attachment{
				Filename:    att.Filename,
				ContentType: att.ContentType,
				Data:        att.Data,
				Inline:      att.Inline,
				ContentId:   att.ContentID,
			})
		}
	}
	return req
}

func protoToEmail(req *pb.SendMailRequest) *Email {
	e := NewEmail()
	e.ID = req.Id
	e.Account = req.Account
	e.From = req.From
	e.FromName = req.FromName
	e.ReplyTo = req.ReplyTo
	e.To = req.To
	e.Cc = req.Cc
	e.Bcc = req.Bcc
	e.Subject = req.Subject
	e.TextBody = req.TextBody
	e.HTMLBody = req.HtmlBody
	e.Headers = req.Headers
	e.Tags = req.Tags
	e.Metadata = req.Metadata
	e.Async = req.Async
	for _, att := range req.Attachments {
		if att != nil {
			e.Attachments = append(e.Attachments, &Attachment{
				Filename:    att.Filename,
				ContentType: att.ContentType,
				Data:        att.Data,
				Inline:      att.Inline,
				ContentID:   att.ContentId,
			})
		}
	}
	return e
}

func sendResponseFromProto(r *pb.SendMailResponse) *SendResponse {
	return &SendResponse{
		ID:      r.Id,
		Status:  r.Status,
		Message: r.Message,
		SentAt:  r.SentAt,
	}
}

func batchResponseFromProto(r *pb.BatchSendMailResponse) *BatchResponse {
	out := &BatchResponse{
		Total:     int(r.Total),
		Succeeded: int(r.Succeeded),
		Failed:    int(r.Failed),
		Results:   make([]*SendResponse, len(r.Results)),
	}
	for i, item := range r.Results {
		if item != nil {
			out.Results[i] = sendResponseFromProto(item)
		}
	}
	return out
}
