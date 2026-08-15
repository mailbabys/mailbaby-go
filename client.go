package mailbaby

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Version of the MailBaby Go client library.
const Version = "0.1.0"

const (
	defaultHeaderName = "X-API-Key"
	defaultTimeout    = 30 * time.Second
	maxResponseBytes  = 32 * 1024 * 1024
)

// Client is a REST client for the MailBaby HTTP API.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	apiKey     string
	headerName string
}

// New creates a Client for the given MailBaby endpoint
// (e.g. "http://localhost:8080").
func New(endpoint string, opts ...Option) (*Client, error) {
	baseURL, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: invalid endpoint %q: %w", endpoint, err)
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("mailbaby: endpoint %q must include scheme and host", endpoint)
	}

	c := &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: defaultTimeout},
		headerName: defaultHeaderName,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return c, nil
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the secret key used for authentication. The key is sent via
// the configured auth header (default "X-API-Key"), which matches the
// MailBaby server's token extraction rules.
func WithAPIKey(key string) Option {
	return func(c *Client) {
		c.apiKey = strings.TrimSpace(key)
	}
}

// WithAuthHeaderName overrides the HTTP header used to carry the API key.
// Defaults to "X-API-Key".
func WithAuthHeaderName(name string) Option {
	return func(c *Client) {
		if strings.TrimSpace(name) != "" {
			c.headerName = strings.TrimSpace(name)
		}
	}
}

// WithHTTPClient replaces the default HTTP client (e.g. for TLS settings or
// custom transports).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		c.httpClient = hc
	}
}

// WithTimeout sets the per-request timeout. Defaults to 30 seconds.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 && c.httpClient != nil {
			c.httpClient.Timeout = d
		}
	}
}

// Endpoint returns the configured base URL.
func (c *Client) Endpoint() string {
	return c.baseURL.String()
}

// Send delivers a single email synchronously (blocking until the server's
// SMTP relay acknowledges) unless Email.Async is set.
func (c *Client) Send(ctx context.Context, email *Email) (*SendResponse, error) {
	if err := email.Validate(); err != nil {
		return nil, err
	}
	var resp SendResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/email/send", email, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendAsync enqueues a single email for asynchronous delivery. It returns as
// soon as the message is accepted by the queue.
func (c *Client) SendAsync(ctx context.Context, email *Email) (*SendResponse, error) {
	if err := email.Validate(); err != nil {
		return nil, err
	}
	email.Async = true
	var resp SendResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/email/send", email, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendBatch delivers multiple emails. If async is true every email is
// enqueued; otherwise each is delivered synchronously with parallel workers
// on the server. Per-item failures are reported in BatchResponse.Results
// rather than returned as an error.
func (c *Client) SendBatch(ctx context.Context, emails []*Email, async bool) (*BatchResponse, error) {
	if len(emails) == 0 {
		return nil, ErrEmptyBatch
	}
	for _, e := range emails {
		if err := e.Validate(); err != nil {
			return nil, err
		}
	}
	req := BatchSendEmailRequest{Emails: emails, Async: async}
	var resp BatchResponse
	if err := c.doJSON(ctx, http.MethodPost, "/v1/email/batch", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Livez checks the service liveness probe.
func (c *Client) Livez(ctx context.Context) (*HealthStatus, error) {
	var status HealthStatus
	if err := c.doJSON(ctx, http.MethodGet, "/livez", nil, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// Readyz checks the service readiness probe. Both 200 (UP) and 503 (DOWN)
// responses carry a valid HealthStatus body; a DOWN status is reflected in
// the returned value rather than as an error.
func (c *Client) Readyz(ctx context.Context) (*HealthStatus, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/readyz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: readyz request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("mailbaby: failed to read readyz response: %w", err)
	}

	var status HealthStatus
	if err := json.Unmarshal(body, &status); err != nil {
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusServiceUnavailable {
			return nil, fmt.Errorf("mailbaby: invalid readyz response: %w", err)
		}
		return nil, apiErrorFromResponse(resp.StatusCode, body)
	}
	return &status, nil
}

// Healthz performs a plain-text health probe returning the server body.
func (c *Client) Healthz(ctx context.Context) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/healthz", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("mailbaby: healthz request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", fmt.Errorf("mailbaby: failed to read healthz response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", apiErrorFromResponse(resp.StatusCode, body)
	}
	return strings.TrimSpace(string(body)), nil
}

func (c *Client) doJSON(ctx context.Context, method, path string, body, out any) error {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("mailbaby: %s %s failed: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("mailbaby: failed to read %s response: %w", path, err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return apiErrorFromResponse(resp.StatusCode, raw)
	}

	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("mailbaby: failed to decode %s response: %w", path, err)
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("mailbaby: failed to encode request body: %w", err)
		}
		reader = bytes.NewReader(data)
	}

	u := *c.baseURL
	u.Path = strings.TrimSuffix(u.Path, "/") + path

	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("mailbaby: failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "mailbaby-go/"+Version)
	if c.apiKey != "" {
		req.Header.Set(c.headerName, c.apiKey)
	}
	return req, nil
}

func apiErrorFromResponse(statusCode int, raw []byte) error {
	var body ErrorResponse
	if err := json.Unmarshal(raw, &body); err != nil {
		return &APIError{
			StatusCode: statusCode,
			Code:       http.StatusText(statusCode),
			Message:    strings.TrimSpace(string(raw)),
		}
	}
	return &APIError{
		StatusCode: statusCode,
		Code:       body.Error,
		Message:    body.Message,
		Details:    body.Details,
	}
}
