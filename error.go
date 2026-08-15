package mailbaby

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Sentinel errors returned by local validation.
var (
	// ErrNilEmail indicates a nil email reference.
	ErrNilEmail = errors.New("mailbaby: email is nil")
	// ErrNoRecipients indicates an email without To/Cc/Bcc recipients.
	ErrNoRecipients = errors.New("mailbaby: no recipients")
	// ErrInvalidFrom indicates an invalid From address.
	ErrInvalidFrom = errors.New("mailbaby: invalid from address")
	// ErrInvalidRecipient indicates an invalid recipient address.
	ErrInvalidRecipient = errors.New("mailbaby: invalid recipient address")
	// ErrNilMessage indicates a nil queue message.
	ErrNilMessage = errors.New("mailbaby: queue message is nil")
	// ErrEmptyBatch indicates an empty email batch.
	ErrEmptyBatch = errors.New("mailbaby: empty batch")
)

// APIError represents an error returned by the MailBaby HTTP API.
// The error body mirrors the server schema: {"code", "error", "details"}.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Details    string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	var b strings.Builder
	b.WriteString("mailbaby: ")
	if e.StatusCode > 0 {
		fmt.Fprintf(&b, "http %d ", e.StatusCode)
	}
	b.WriteString(e.Code)
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	if e.Details != "" {
		fmt.Fprintf(&b, " (%s)", e.Details)
	}
	return b.String()
}

// Is allows errors.Is(err, ErrUnauthorized) style classification.
func (e *APIError) Is(target error) bool {
	switch target {
	case ErrUnauthorized:
		return e.StatusCode == http.StatusUnauthorized
	case ErrValidation:
		return e.StatusCode == http.StatusBadRequest
	case ErrDeliveryFailed:
		return e.StatusCode == http.StatusInternalServerError
	}
	return false
}

// Sentinel errors for APIError classification via errors.Is.
var (
	// ErrUnauthorized indicates an invalid or missing authentication token.
	ErrUnauthorized = errors.New("mailbaby: unauthorized")
	// ErrValidation indicates the request payload failed server-side validation.
	ErrValidation = errors.New("mailbaby: validation error")
	// ErrDeliveryFailed indicates a synchronous delivery failure on the server.
	ErrDeliveryFailed = errors.New("mailbaby: delivery failed")
)

// ErrorResponse is the JSON error payload returned by the MailBaby HTTP API.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
	Details string `json:"details,omitempty"`
}
