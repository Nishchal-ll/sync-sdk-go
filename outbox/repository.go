package outbox

import (
	"context"
	"errors"
)

var (
	ErrItemNotFound = errors.New("outbox: item not found")
)

// Repository is the storage interface client applications implement against their local database (SQLite, Postgres, etc.).
type Repository interface {
	// FetchPending retrieves pending outbox items up to the specified limit.
	FetchPending(ctx context.Context, limit int) ([]OutboxItem, error)

	// MarkPublished marks an outbox item as successfully published.
	MarkPublished(ctx context.Context, id string) error

	// MarkFailed marks an outbox item as failed and updates retry count and error message.
	MarkFailed(ctx context.Context, id string, errStr string) error
}
