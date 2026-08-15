package mailbaby

import (
	"encoding/json"
	"time"
)

// Delivery status constants returned by the MailBaby service.
const (
	StatusSent   = "sent"   // email delivered synchronously
	StatusQueued = "queued" // email accepted and enqueued for async delivery
	StatusFailed = "failed" // individual batch item failure
)

// SendResponse represents the JSON response of a single email delivery
// operation (POST /v1/email/send).
type SendResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"` // "sent" or "queued"
	Message string `json:"message"`
	SentAt  int64  `json:"sent_at"` // Unix timestamp in nanoseconds
}

// SentAtTime converts the nanosecond SentAt timestamp to a time.Time.
func (r *SendResponse) SentAtTime() time.Time {
	return time.Unix(0, r.SentAt)
}

// BatchSendEmailRequest is the wire payload of POST /v1/email/batch.
type BatchSendEmailRequest struct {
	Emails []*Email `json:"emails"`
	Async  bool     `json:"async,omitempty"`
}

// BatchResponse represents the result of a batch delivery
// (POST /v1/email/batch).
type BatchResponse struct {
	Total     int             `json:"total"`
	Succeeded int             `json:"succeeded"`
	Failed    int             `json:"failed"`
	Results   []*SendResponse `json:"results"`
}

// Successful returns the per-item responses for emails that were sent or
// queued successfully.
func (r *BatchResponse) Successful() []*SendResponse {
	var out []*SendResponse
	for _, res := range r.Results {
		if res != nil && res.Status != StatusFailed {
			out = append(out, res)
		}
	}
	return out
}

// Failures returns the per-item responses for emails that failed.
func (r *BatchResponse) Failures() []*SendResponse {
	var out []*SendResponse
	for _, res := range r.Results {
		if res != nil && res.Status == StatusFailed {
			out = append(out, res)
		}
	}
	return out
}

// HealthStatus is the JSON payload of the /livez and /readyz endpoints.
type HealthStatus struct {
	Status     string            `json:"status"` // "UP" or "DOWN"
	Components map[string]string `json:"components,omitempty"`
	Timestamp  string            `json:"timestamp"`
}

func decodeJSON(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
