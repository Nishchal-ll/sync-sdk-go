package syncstate

import (
	"time"
)

// Checkpoint represents persistent synchronization watermark state for a durable consumer.
type Checkpoint struct {
	ConsumerID string    `json:"consumer_id"` // Durable consumer ID (e.g. "pos_01_catalog")
	Stream     string    `json:"stream"`      // NATS JetStream stream name (e.g. "ZTT_CATALOG")
	Sequence   uint64    `json:"sequence"`    // Last successfully processed JetStream sequence number
	UpdatedAt  time.Time `json:"updated_at"`  // UTC timestamp of last checkpoint commit
}

// NewCheckpoint constructs a new Checkpoint instance.
func NewCheckpoint(consumerID, stream string, sequence uint64) Checkpoint {
	return Checkpoint{
		ConsumerID: consumerID,
		Stream:     stream,
		Sequence:   sequence,
		UpdatedAt:  time.Now().UTC(),
	}
}
