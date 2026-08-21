package conflict

import (
	"context"

	"github.com/biticonic/sync-sdk-go/event"
)

// Resolution indicates how a concurrent conflict should be handled.
type Resolution int

const (
	AcceptIncoming Resolution = iota // Incoming remote envelope overwrites local state
	KeepLocal                        // Local state is preserved; incoming remote envelope is discarded
	Merge                            // A custom merged envelope (returned) is applied
	Reject                           // Conflict is unresolvable; reject processing and trigger alert
)

// ConflictResolver is the interface applications implement to resolve concurrent branch divergence.
type ConflictResolver interface {
	// Resolve handles concurrent divergence between local and incoming EventEnvelopes.
	// For Merge resolution, the returned *event.EventEnvelope must contain the merged payload.
	Resolve(ctx context.Context, local, incoming event.EventEnvelope) (Resolution, *event.EventEnvelope, error)
}

// ConflictResolverFunc allows function callbacks to act as ConflictResolvers.
type ConflictResolverFunc func(ctx context.Context, local, incoming event.EventEnvelope) (Resolution, *event.EventEnvelope, error)

func (f ConflictResolverFunc) Resolve(ctx context.Context, local, incoming event.EventEnvelope) (Resolution, *event.EventEnvelope, error) {
	return f(ctx, local, incoming)
}
