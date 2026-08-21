package inbox

import (
	"time"
)

// InboxItem represents a recorded message entry in the local application inbox ledger.
type InboxItem struct {
	MessageID   string    `json:"message_id"`   // UUIDv4 matching EventEnvelope.ID
	HandlerName string    `json:"handler_name"` // Domain handler name that processed the event
	ProcessedAt time.Time `json:"processed_at"` // UTC timestamp when transaction committed
}

// NewInboxItem creates a new InboxItem entry.
func NewInboxItem(messageID, handlerName string) InboxItem {
	return InboxItem{
		MessageID:   messageID,
		HandlerName: handlerName,
		ProcessedAt: time.Now().UTC(),
	}
}
