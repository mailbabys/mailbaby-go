package mailbaby

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testClient(t *testing.T, srv *httptest.Server, opts ...Option) *Client {
	t.Helper()
	c, err := New(srv.URL, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestNewEndpointValidation(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
	if _, err := New("localhost:8080"); err == nil {
		t.Fatal("expected error for endpoint without scheme")
	}
	c, err := New("http://localhost:8080", WithAPIKey("secret"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Endpoint() != "http://localhost:8080" {
		t.Fatalf("unexpected endpoint %q", c.Endpoint())
	}
}

func TestClientSend(t *testing.T) {
	var gotPath, gotMethod, gotAuthHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotMethod = r.Method
		gotAuthHeader = r.Header.Get("X-API-Key")

		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if req["subject"] != "Order Confirmation #10024" {
			t.Errorf("unexpected subject %v", req["subject"])
		}
		if req["to"] == nil {
			t.Error("expected to field")
		}
		if _, ok := req["async"]; ok {
			t.Error("async must be omitted for sync send")
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"e8a93bf84c379a20","status":"sent","message":"email sent successfully","sent_at":1771142400000000000}`))
	}))
	defer srv.Close()

	c := testClient(t, srv, WithAPIKey("secret"))
	email := NewEmail().
		SetAccount("default").
		SetFrom("noreply@example.com", "MailBaby System").
		AddTo("alice@example.com").
		SetSubject("Order Confirmation #10024").
		SetTextBody("Thank you for your order!").
		AddTag("order")

	resp, err := c.Send(context.Background(), email)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp.ID != "e8a93bf84c379a20" || resp.Status != StatusSent {
		t.Errorf("unexpected response: %+v", resp)
	}
	if gotPath != "/v1/email/send" || gotMethod != http.MethodPost {
		t.Errorf("unexpected request: %s %s", gotMethod, gotPath)
	}
	if gotAuthHeader != "secret" {
		t.Errorf("expected X-API-Key header, got %q", gotAuthHeader)
	}
}

func TestClientSendAsync(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":"abc","status":"queued","message":"email enqueued successfully","sent_at":1}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	resp, err := c.SendAsync(context.Background(), NewEmail().AddTo("bob@example.com").SetSubject("Hi"))
	if err != nil {
		t.Fatalf("SendAsync: %v", err)
	}
	if resp.Status != StatusQueued {
		t.Errorf("expected queued status, got %q", resp.Status)
	}
	if async, ok := gotBody["async"].(bool); !ok || !async {
		t.Errorf("expected async=true in body, got %v", gotBody["async"])
	}
}

func TestClientSendBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req BatchSendEmailRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if len(req.Emails) != 2 || !req.Async {
			t.Errorf("unexpected batch request: %+v", req)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"total": 2, "succeeded": 1, "failed": 1,
			"results": [
				{"id":"ok1","status":"sent","message":"email sent successfully","sent_at":1},
				{"id":"bad1","status":"failed","message":"delivery failed: smtp error","sent_at":2}
			]
		}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	batch, err := c.SendBatch(context.Background(), []*Email{
		NewEmail().AddTo("user1@example.com").SetSubject("A"),
		NewEmail().AddTo("user2@example.com").SetSubject("B"),
	}, true)
	if err != nil {
		t.Fatalf("SendBatch: %v", err)
	}
	if batch.Total != 2 || batch.Succeeded != 1 || batch.Failed != 1 {
		t.Errorf("unexpected batch summary: %+v", batch)
	}
	if len(batch.Successful()) != 1 || len(batch.Failures()) != 1 {
		t.Errorf("unexpected filtered results")
	}
}

func TestClientSendAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":400,"error":"validation_error","details":"invalid recipient"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	_, err := c.Send(context.Background(), NewEmail().AddTo("x@example.com").SetSubject("Hi"))
	if err == nil {
		t.Fatal("expected error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.StatusCode != 400 || apiErr.Code != "validation_error" || apiErr.Details != "invalid recipient" {
		t.Errorf("unexpected APIError: %+v", apiErr)
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected errors.Is(ErrValidation)")
	}
}

func TestClientUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"code":401,"error":"unauthorized","message":"invalid or missing authentication token / secret key"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	_, err := c.Send(context.Background(), NewEmail().AddTo("x@example.com").SetSubject("Hi"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Errorf("expected errors.Is(ErrUnauthorized), got %v", err)
	}
}

func TestClientDeliveryFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"code":500,"error":"delivery_failed","details":"smtp: 550 5.1.1 rejected"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	_, err := c.Send(context.Background(), NewEmail().AddTo("x@example.com").SetSubject("Hi"))
	if !errors.Is(err, ErrDeliveryFailed) {
		t.Errorf("expected errors.Is(ErrDeliveryFailed), got %v", err)
	}
}

func TestClientValidation(t *testing.T) {
	c := testClient(t, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called")
	})))

	_, err := c.Send(context.Background(), &Email{})
	if !errors.Is(err, ErrNoRecipients) {
		t.Errorf("expected ErrNoRecipients, got %v", err)
	}

	bad := NewEmail().AddTo("not-an-address")
	_, err = c.Send(context.Background(), bad)
	if !errors.Is(err, ErrInvalidRecipient) {
		t.Errorf("expected ErrInvalidRecipient, got %v", err)
	}
}

func TestClientHealth(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"UP","timestamp":"2026-01-01T00:00:00Z"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("down") == "1" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"DOWN","components":{"sender":"DOWN: smtp unreachable"},"timestamp":"2026-01-01T00:00:00Z"}`))
			return
		}
		_, _ = w.Write([]byte(`{"status":"UP","components":{"sender":"UP","queue":"UP"},"timestamp":"2026-01-01T00:00:00Z"}`))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK\n"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := testClient(t, srv)

	live, err := c.Livez(context.Background())
	if err != nil || live.Status != "UP" {
		t.Fatalf("Livez: %+v err=%v", live, err)
	}

	ready, err := c.Readyz(context.Background())
	if err != nil || ready.Status != "UP" || ready.Components["queue"] != "UP" {
		t.Fatalf("Readyz: %+v err=%v", ready, err)
	}

	text, err := c.Healthz(context.Background())
	if err != nil || text != "OK" {
		t.Fatalf("Healthz: %q err=%v", text, err)
	}
}

func TestClientReadyzDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"DOWN","components":{"queue":"DOWN: broker unreachable"},"timestamp":"x"}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	status, err := c.Readyz(context.Background())
	if err != nil {
		t.Fatalf("Readyz should not error on 503: %v", err)
	}
	if status.Status != "DOWN" {
		t.Errorf("expected DOWN status, got %q", status.Status)
	}
}

func TestClientCustomAuthHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Secret-Token")
		_, _ = w.Write([]byte(`{"id":"x","status":"sent","message":"ok","sent_at":1}`))
	}))
	defer srv.Close()

	c := testClient(t, srv, WithAPIKey("s3cret"), WithAuthHeaderName("X-Secret-Token"))
	_, err := c.Send(context.Background(), NewEmail().AddTo("a@b.com").SetSubject("s"))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got != "s3cret" {
		t.Errorf("expected custom auth header, got %q", got)
	}
}

func TestClientSendRoundTripAttachment(t *testing.T) {
	var req Email
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&req)
		_, _ = w.Write([]byte(`{"id":"1","status":"sent","message":"ok","sent_at":1}`))
	}))
	defer srv.Close()

	c := testClient(t, srv)
	email := NewEmail().
		AddTo("user@example.com").
		SetSubject("Invoice").
		Attach("invoice.pdf", []byte("%PDF-1.4"), "application/pdf").
		AttachInline("logo.png", "logo_img", []byte{0x89, 0x50})

	if _, err := c.Send(context.Background(), email); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(req.Attachments) != 2 {
		t.Fatalf("expected 2 attachments, got %d", len(req.Attachments))
	}
	if req.Attachments[0].ContentType != "application/pdf" || req.Attachments[0].Inline {
		t.Errorf("unexpected attachment 0: %+v", req.Attachments[0])
	}
	if !req.Attachments[1].Inline || req.Attachments[1].ContentID != "logo_img" {
		t.Errorf("unexpected attachment 1: %+v", req.Attachments[1])
	}
}

func TestClientServerDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close()

	c, err := New(addr)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.Send(context.Background(), NewEmail().AddTo("a@b.com").SetSubject("s"))
	if err == nil {
		t.Fatal("expected transport error")
	}
	if errors.Is(err, ErrValidation) {
		t.Fatalf("transport error misclassified: %v", err)
	}
}

func TestEmailJSONWireFormat(t *testing.T) {
	email := NewEmail().
		SetAccount("marketing").
		SetFrom("news@example.com", "News").
		SetReplyTo("support@example.com").
		AddTo("a@example.com").
		AddCc("b@example.com").
		AddBcc("c@example.com").
		SetSubject("Subject").
		SetTextBody("text").
		SetHTMLBody("<p>html</p>").
		SetHeader("X-Custom", "v").
		SetMetadata("env", "prod").
		AddTag("newsletter")

	data, err := email.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"account", "from", "from_name", "reply_to", "to", "cc", "bcc", "subject", "text_body", "html_body", "headers", "tags", "metadata"} {
		if _, ok := m[key]; !ok {
			t.Errorf("missing wire field %q in %s", key, data)
		}
	}
	if m["async"] != nil {
		t.Errorf("async must be omitted when false")
	}

	var round Email
	if err := round.FromJSON(data); err != nil {
		t.Fatalf("FromJSON: %v", err)
	}
	if round.Account != "marketing" || len(round.To) != 1 || round.Headers["X-Custom"] != "v" {
		t.Errorf("round trip mismatch: %+v", round)
	}
}

func TestEmailValidate(t *testing.T) {
	if err := NewEmail().AddTo("a@b.com").SetSubject("x").Validate(); err != nil {
		t.Errorf("valid email rejected: %v", err)
	}
	if err := (&Email{}).Validate(); !errors.Is(err, ErrNoRecipients) {
		t.Errorf("expected ErrNoRecipients, got %v", err)
	}
	if err := (NewEmail().SetFrom("bad-address").AddTo("a@b.com")).Validate(); !errors.Is(err, ErrInvalidFrom) {
		t.Errorf("expected ErrInvalidFrom, got %v", err)
	}
}

func TestAPIErrorString(t *testing.T) {
	err := &APIError{StatusCode: 401, Code: "unauthorized", Message: "bad token", Details: "x"}
	msg := err.Error()
	for _, want := range []string{"401", "unauthorized", "bad token"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q missing %q", msg, want)
		}
	}
	if errors.Is(err, ErrUnauthorized) == false {
		t.Error("expected Is(ErrUnauthorized)")
	}
}

func TestHealthStatusStructs(t *testing.T) {
	resp := &SendResponse{ID: "1", Status: StatusQueued, SentAt: 1771142400000000000}
	if resp.SentAtTime().IsZero() {
		t.Error("expected nonzero SentAtTime")
	}

	var s HealthStatus
	if err := decodeJSON([]byte(`{"status":"UP","timestamp":"x"}`), &s); err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
	if s.Status != "UP" {
		t.Errorf("unexpected status %q", s.Status)
	}
}

func TestSendBatchEmpty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be called")
	}))
	defer srv.Close()
	c := testClient(t, srv)
	_, err := c.SendBatch(context.Background(), nil, false)
	if !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}
}
