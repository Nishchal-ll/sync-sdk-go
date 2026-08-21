package snapshot

import (
	"encoding/json"
	"time"
)

// Snapshot represents materialized synchronization state at a specific stream watermark.
type Snapshot struct {
	Entity    string          `json:"entity"`     // Entity identifier (e.g., "catalog", "inventory")
	Data      json.RawMessage `json:"data"`       // Opaque snapshot payload rendered by application
	StreamSeq uint64          `json:"stream_seq"` // Last JetStream sequence number reflected in Data
	CreatedAt time.Time       `json:"created_at"` // UTC timestamp of snapshot rendering
}

// NewSnapshot constructs a new Snapshot instance.
func NewSnapshot(entity string, data json.RawMessage, streamSeq uint64) Snapshot {
	return Snapshot{
		Entity:    entity,
		Data:      data,
		StreamSeq: streamSeq,
		CreatedAt: time.Now().UTC(),
	}
}
