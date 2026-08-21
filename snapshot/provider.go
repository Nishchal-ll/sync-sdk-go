package snapshot

import (
	"context"
)

// SnapshotProvider is the interface implemented by consumer applications to produce and restore cold-start snapshots.
type SnapshotProvider interface {
	// CreateSnapshot renders the current domain state and returns it along with the stream watermark sequence.
	CreateSnapshot(ctx context.Context, entity string) (Snapshot, error)

	// RestoreSnapshot imports a rendered domain snapshot into the local application database.
	RestoreSnapshot(ctx context.Context, snapshot Snapshot) error
}
