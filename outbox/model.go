package outbox

import (
	"time"

	"github.com/biticonic/sync-sdk-go/event"
)

const (
	StatusPending   = "PENDING"
	StatusPublished = "PUBLISHED"
	StatusFailed    = "FAILED"
)

// OutboxItem represents a single event stored in the local outbox ledger.
type OutboxItem struct {
	ID         string              `json:"id"`
	Envelope   event.EventEnvelope `json:"envelope"`
	Status     string              `json:"status"`
	RetryCount int                 `json:"retry_count"`
	LastError  string              `json:"last_error,omitempty"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`
}

// NewOutboxItem wraps an EventEnvelope into an OutboxItem.
func NewOutboxItem(env event.EventEnvelope) OutboxItem {
	now := time.Now().UTC()
	return OutboxItem{
		ID:        env.ID,
		Envelope:  env,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}
