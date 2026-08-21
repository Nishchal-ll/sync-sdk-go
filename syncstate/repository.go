package syncstate

import (
	"context"
	"errors"

	"github.com/biticonic/sync-sdk-go/inbox"
)

var (
	ErrCheckpointNotFound = errors.New("syncstate: checkpoint not found")
)

// CheckpointRepository is the storage interface client applications implement against their local database.
type CheckpointRepository interface {
	// GetCheckpoint retrieves the last committed sequence for a consumer and stream.
	GetCheckpoint(ctx context.Context, consumerID, stream string) (Checkpoint, error)

	// SaveCheckpoint records/updates the checkpoint sequence inside an optional database transaction handle.
	SaveCheckpoint(ctx context.Context, tx inbox.Transaction, checkpoint Checkpoint) error
}
