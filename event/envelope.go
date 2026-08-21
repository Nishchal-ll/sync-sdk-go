package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrMissingAppID    = errors.New("event: missing app_id")
	ErrMissingNodeID   = errors.New("event: missing node_id")
	ErrMissingEntity   = errors.New("event: missing entity")
	ErrMissingEntityID = errors.New("event: missing entity_id")
	ErrMissingAction   = errors.New("event: missing action")
)

// EventEnvelope is the universal, domain-agnostic container for all synchronization events.
type EventEnvelope struct {
	ID            string           `json:"id"`                       // Unique Event UUID (used for Nats-Msg-Id dedup)
	AppID         string           `json:"app_id"`                   // Application Namespace (e.g., "ztt", "pharmacy")
	TenantID      string           `json:"tenant_id,omitempty"`      // Optional Tenant Scope
	NodeID        string           `json:"node_id"`                  // Source Node ID (e.g., "01cc92a6-...", "cloud")
	Entity        string           `json:"entity"`                   // Entity Type (e.g., "order", "product", "inventory")
	EntityID      string           `json:"entity_id"`                // Primary Key of the entity
	Action        string           `json:"action"`                   // Action performed (e.g., "created", "adjusted")
	EntityVersion int64            `json:"entity_version,omitempty"` // Monotonic Version (Single-Writer)
	VectorClock   map[string]int64 `json:"vector_clock,omitempty"`   // Causal Vector Clock (Multi-Writer State)
	HLC           string           `json:"hlc,omitempty"`            // Hybrid Logical Clock timestamp
	Timestamp     time.Time        `json:"timestamp"`                // Event Creation Timestamp (UTC)
	Schema        string           `json:"schema"`                   // JSON Schema Identifier (e.g., "com.ztt.order.created")
	Version       string           `json:"version"`                  // Envelope Schema Version (e.g., "1.0")
	CorrelationID string           `json:"correlation_id,omitempty"` // Tracing Correlation ID
	CausationID   string           `json:"causation_id,omitempty"`   // Tracing Causation ID
	Payload       json.RawMessage  `json:"payload"`                  // Opaque Application Data
}

// NewEnvelope constructs a new EventEnvelope with a generated UUIDv4 and UTC timestamp.
func NewEnvelope(appID, nodeID, entity, entityID, action string, payload []byte) EventEnvelope {
	return EventEnvelope{
		ID:        uuid.New().String(),
		AppID:     appID,
		NodeID:    nodeID,
		Entity:    entity,
		EntityID:  entityID,
		Action:    action,
		Timestamp: time.Now().UTC(),
		Version:   "1.0",
		Payload:   payload,
	}
}

// Validate checks that all mandatory routing metadata fields are populated.
func (e EventEnvelope) Validate() error {
	if e.AppID == "" {
		return ErrMissingAppID
	}
	if e.NodeID == "" {
		return ErrMissingNodeID
	}
	if e.Entity == "" {
		return ErrMissingEntity
	}
	if e.EntityID == "" {
		return ErrMissingEntityID
	}
	if e.Action == "" {
		return ErrMissingAction
	}
	return nil
}

// Subject builds the canonical NATS subject for this event: <app>.<source>.<entity>.<action>
func (e EventEnvelope) Subject() string {
	return fmt.Sprintf("%s.%s.%s.%s", e.AppID, e.NodeID, e.Entity, e.Action)
}
