// Package mailbaby provides a Go client for the MailBaby email delivery
// microservice. It supports the HTTP REST API, the gRPC MailService and
// direct message-queue publishing.
package mailbaby

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/mail"
	"path/filepath"
	"strings"
)

// Attachment represents a file attached to an email, either as a regular
// attachment or inline CID resource.
type Attachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
	Inline      bool   `json:"inline"`
	ContentID   string `json:"content_id,omitempty"` // for <img src="cid:xyz">
}

// Email represents an email message to be delivered by the MailBaby service.
// The JSON field names match the MailBaby wire contract shared by the REST
// API, the gRPC service and message-queue payloads.
type Email struct {
	ID          string            `json:"id,omitempty"`
	Account     string            `json:"account,omitempty"` // Target SMTP account (empty for default)
	From        string            `json:"from,omitempty"`    // Override account default From
	FromName    string            `json:"from_name,omitempty"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	To          []string          `json:"to"`
	Cc          []string          `json:"cc,omitempty"`
	Bcc         []string          `json:"bcc,omitempty"`
	Subject     string            `json:"subject"`
	TextBody    string            `json:"text_body,omitempty"`
	HTMLBody    string            `json:"html_body,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attachments []*Attachment     `json:"attachments,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Async       bool              `json:"async,omitempty"` // REST only: enqueue instead of synchronous send
}

// NewEmail creates an initialized Email struct.
func NewEmail() *Email {
	return &Email{
		Headers:     make(map[string]string),
		Metadata:    make(map[string]string),
		Attachments: make([]*Attachment, 0),
	}
}

// SetID sets a custom message ID. An empty ID makes MailBaby generate one.
func (e *Email) SetID(id string) *Email {
	e.ID = strings.TrimSpace(id)
	return e
}

// SetAccount sets the target SMTP account name for sending.
func (e *Email) SetAccount(account string) *Email {
	e.Account = strings.TrimSpace(account)
	return e
}

// SetFrom sets the sender's email address and optional display name.
func (e *Email) SetFrom(from string, name ...string) *Email {
	e.From = strings.TrimSpace(from)
	if len(name) > 0 {
		e.FromName = strings.TrimSpace(name[0])
	}
	return e
}

// SetReplyTo sets the Reply-To address.
func (e *Email) SetReplyTo(replyTo string) *Email {
	e.ReplyTo = strings.TrimSpace(replyTo)
	return e
}

// AddTo adds one or more primary recipients.
func (e *Email) AddTo(addresses ...string) *Email {
	e.To = appendTrimmed(e.To, addresses...)
	return e
}

// AddCc adds one or more carbon-copy recipients.
func (e *Email) AddCc(addresses ...string) *Email {
	e.Cc = appendTrimmed(e.Cc, addresses...)
	return e
}

// AddBcc adds one or more blind carbon-copy recipients.
func (e *Email) AddBcc(addresses ...string) *Email {
	e.Bcc = appendTrimmed(e.Bcc, addresses...)
	return e
}

// SetSubject sets the email subject line.
func (e *Email) SetSubject(subject string) *Email {
	e.Subject = subject
	return e
}

// SetTextBody sets the plain-text body content.
func (e *Email) SetTextBody(body string) *Email {
	e.TextBody = body
	return e
}

// SetHTMLBody sets the rich HTML body content.
func (e *Email) SetHTMLBody(body string) *Email {
	e.HTMLBody = body
	return e
}

// SetHeader sets a custom MIME header.
func (e *Email) SetHeader(key, value string) *Email {
	if e.Headers == nil {
		e.Headers = make(map[string]string)
	}
	e.Headers[key] = value
	return e
}

// SetMetadata sets a custom metadata key-value pair.
func (e *Email) SetMetadata(key, value string) *Email {
	if e.Metadata == nil {
		e.Metadata = make(map[string]string)
	}
	e.Metadata[key] = value
	return e
}

// AddTag appends a tag to the email.
func (e *Email) AddTag(tag string) *Email {
	if tag = strings.TrimSpace(tag); tag != "" {
		e.Tags = append(e.Tags, tag)
	}
	return e
}

// SetAsync marks the email for asynchronous queue ingestion (REST only).
func (e *Email) SetAsync(async bool) *Email {
	e.Async = async
	return e
}

// Attach attaches a file to the email.
func (e *Email) Attach(filename string, data []byte, contentType ...string) *Email {
	e.Attachments = append(e.Attachments, &Attachment{
		Filename:    filename,
		ContentType: detectContentType(filename, contentType...),
		Data:        data,
		Inline:      false,
	})
	return e
}

// AttachInline attaches an inline file (e.g. image with Content-ID for HTML
// <img src="cid:xxx">).
func (e *Email) AttachInline(filename, contentID string, data []byte, contentType ...string) *Email {
	e.Attachments = append(e.Attachments, &Attachment{
		Filename:    filename,
		ContentType: detectContentType(filename, contentType...),
		Data:        data,
		Inline:      true,
		ContentID:   strings.Trim(contentID, "<>"),
	})
	return e
}

// Validate checks that the email has the essential fields before sending.
func (e *Email) Validate() error {
	if e == nil {
		return ErrNilEmail
	}

	if len(e.To) == 0 && len(e.Cc) == 0 && len(e.Bcc) == 0 {
		return ErrNoRecipients
	}

	if e.From != "" {
		if _, err := mail.ParseAddress(e.From); err != nil {
			return fmt.Errorf("%w: %q (%v)", ErrInvalidFrom, e.From, err)
		}
	}

	if err := validateAddresses(e.To, "To"); err != nil {
		return err
	}
	if err := validateAddresses(e.Cc, "Cc"); err != nil {
		return err
	}
	if err := validateAddresses(e.Bcc, "Bcc"); err != nil {
		return err
	}

	if e.ReplyTo != "" {
		if _, err := mail.ParseAddress(e.ReplyTo); err != nil {
			return fmt.Errorf("mailbaby: invalid reply_to email %q: %w", e.ReplyTo, err)
		}
	}

	return nil
}

// ToJSON serializes the email to the MailBaby wire format.
func (e *Email) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON deserializes MailBaby wire-format JSON into the email struct.
func (e *Email) FromJSON(data []byte) error {
	return json.Unmarshal(data, e)
}

func appendTrimmed(list []string, addresses ...string) []string {
	for _, addr := range addresses {
		if trimmed := strings.TrimSpace(addr); trimmed != "" {
			list = append(list, trimmed)
		}
	}
	return list
}

func validateAddresses(list []string, field string) error {
	for _, addr := range list {
		if _, err := mail.ParseAddress(addr); err != nil {
			return fmt.Errorf("%w in %s: %q (%v)", ErrInvalidRecipient, field, addr, err)
		}
	}
	return nil
}

func detectContentType(filename string, custom ...string) string {
	if len(custom) > 0 && strings.TrimSpace(custom[0]) != "" {
		return custom[0]
	}
	ext := filepath.Ext(filename)
	if ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			return mimeType
		}
	}
	return "application/octet-stream"
}
