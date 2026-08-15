package mailbaby

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/mailbabys/mailbaby-go/pb"
)

func statusError(code codes.Code, msg string) error {
	return status.Error(code, msg)
}

const bufSize = 1024 * 1024

type fakeMailService struct {
	pb.UnimplementedMailServiceServer
	sent []*pb.SendMailRequest
	auth bool
}

func (f *fakeMailService) Send(ctx context.Context, req *pb.SendMailRequest) (*pb.SendMailResponse, error) {
	if f.auth {
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get("authorization")) == 0 {
			return nil, statusError(codes.Unauthenticated, "missing token")
		}
	}
	f.sent = append(f.sent, req)
	return &pb.SendMailResponse{
		Id:      req.Id,
		Status:  "sent",
		Message: "email sent successfully",
		SentAt:  1771142400000000000,
	}, nil
}

func (f *fakeMailService) SendBatch(ctx context.Context, req *pb.BatchSendMailRequest) (*pb.BatchSendMailResponse, error) {
	resp := &pb.BatchSendMailResponse{Total: int32(len(req.Emails))}
	for i, e := range req.Emails {
		resp.Succeeded++
		resp.Results = append(resp.Results, &pb.SendMailResponse{
			Id:      e.Id,
			Status:  "sent",
			Message: "email sent successfully",
			SentAt:  int64(i) + 1,
		})
	}
	return resp, nil
}

func (f *fakeMailService) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Status: "OK", Version: "dev", Timestamp: 1}, nil
}

func (f *fakeMailService) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	return &pb.HealthCheckResponse{
		Status:  pb.HealthCheckResponse_SERVING,
		Details: map[string]string{"sender": "READY", "queue": "DIRECT_MODE"},
	}, nil
}

func startTestGRPC(t *testing.T, auth bool) *GRPCClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	server := grpc.NewServer()
	pb.RegisterMailServiceServer(server, &fakeMailService{auth: auth})
	go func() {
		_ = server.Serve(lis)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return NewGRPCClient(conn, WithGRPCAuth("secret"))
}

func TestGRPCSend(t *testing.T) {
	c := startTestGRPC(t, false)

	resp, err := c.Send(context.Background(), NewEmail().
		SetFrom("noreply@example.com", "MailBaby").
		AddTo("alice@example.com").
		SetSubject("Order Confirmation").
		Attach("a.pdf", []byte("data")))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.Status != StatusSent {
		t.Errorf("unexpected status %q", resp.Status)
	}
	if resp.SentAtTime().IsZero() {
		t.Error("expected nonzero sent_at")
	}
}

func TestGRPCSendAsync(t *testing.T) {
	c := startTestGRPC(t, false)
	resp, err := c.SendAsync(context.Background(), NewEmail().AddTo("b@c.com").SetSubject("x"))
	if err != nil {
		t.Fatalf("SendAsync: %v", err)
	}
	if resp.Status != StatusSent {
		t.Errorf("unexpected status %q", resp.Status)
	}
}

func TestGRPCSendBatch(t *testing.T) {
	c := startTestGRPC(t, false)
	batch, err := c.SendBatch(context.Background(), []*Email{
		NewEmail().AddTo("a@b.com").SetSubject("1"),
		NewEmail().AddTo("c@d.com").SetSubject("2"),
	}, false)
	if err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if batch.Total != 2 || batch.Succeeded != 2 || len(batch.Results) != 2 {
		t.Errorf("unexpected batch: %+v", batch)
	}
}

func TestGRPCAuthMetadata(t *testing.T) {
	c := startTestGRPC(t, true)
	if _, err := c.Send(context.Background(), NewEmail().AddTo("a@b.com").SetSubject("x")); err != nil {
		t.Fatalf("Send with auth: %v", err)
	}
}

func TestGRPCPing(t *testing.T) {
	c := startTestGRPC(t, false)
	pong, err := c.Ping(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if pong.Status != "OK" {
		t.Errorf("unexpected ping status %q", pong.Status)
	}
}

func TestGRPCHealthCheck(t *testing.T) {
	c := startTestGRPC(t, false)
	health, err := c.HealthCheck(context.Background(), "")
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if health.Status != ServingStatusServing {
		t.Errorf("unexpected status %v", health.Status)
	}
	if health.Details["sender"] != "READY" {
		t.Errorf("unexpected details: %+v", health.Details)
	}
}

func TestEmailProtoRoundTrip(t *testing.T) {
	email := NewEmail().
		SetAccount("ops").
		SetFrom("a@b.com").
		SetReplyTo("r@b.com").
		AddTo("t@b.com").
		AddCc("c@b.com").
		AddBcc("x@b.com").
		SetSubject("S").
		SetTextBody("t").
		SetHTMLBody("h").
		SetHeader("X-K", "v").
		SetMetadata("m", "1").
		AddTag("tag").
		AttachInline("img.png", "cid1", []byte{1, 2, 3}, "image/png")

	proto := emailToProto(email)
	back := protoToEmail(proto)

	if back.Account != email.Account || back.From != email.From || back.Subject != email.Subject {
		t.Errorf("round trip mismatch: %+v", back)
	}
	if len(back.To) != 1 || len(back.Cc) != 1 || len(back.Bcc) != 1 {
		t.Errorf("recipient mismatch: %+v", back)
	}
	if len(back.Attachments) != 1 || back.Attachments[0].ContentID != "cid1" || !back.Attachments[0].Inline {
		t.Errorf("attachment mismatch: %+v", back.Attachments)
	}
	if back.Headers["X-K"] != "v" || back.Metadata["m"] != "1" || len(back.Tags) != 1 {
		t.Errorf("misc mismatch: %+v", back)
	}
}

func TestGRPCErrorCode(t *testing.T) {
	code, ok := GRPCErrorCode(statusError(codes.Internal, "boom"))
	if !ok || code != codes.Internal {
		t.Errorf("expected Internal code, got %v (%v)", code, ok)
	}
	if _, ok := GRPCErrorCode(nil); ok {
		t.Error("nil error should not have a code")
	}
}
