package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	natsTransport "github.com/biticonic/sync-sdk-go/transport/nats"
	"github.com/nats-io/nats.go"
)

// RequestPayload represents the request data sent to request a snapshot.
type RequestPayload struct {
	Entity string `json:"entity"`
}

// Coordinator handles NATS request-reply snapshot bootstrap coordination.
type Coordinator struct {
	natsClient *natsTransport.Client
	appID      string
	tenantID   string
}

// NewCoordinator creates a new Snapshot Coordinator.
func NewCoordinator(natsClient *natsTransport.Client, appID, tenantID string) *Coordinator {
	return &Coordinator{
		natsClient: natsClient,
		appID:      appID,
		tenantID:   tenantID,
	}
}

// RequestSubject builds the NATS request subject: <app>.<tenant>.cloud.snapshot.request
func (c *Coordinator) RequestSubject() string {
	return fmt.Sprintf("%s.%s.cloud.snapshot.request", c.appID, c.tenantID)
}

// ListenForRequests starts a cloud-side responder listening for snapshot requests and serving SnapshotProvider dumps.
func (c *Coordinator) ListenForRequests(ctx context.Context, provider SnapshotProvider) (*nats.Subscription, error) {
	nc := c.natsClient.Conn()
	if nc == nil {
		return nil, fmt.Errorf("snapshot coordinator: NATS connection is nil")
	}

	subject := c.RequestSubject()

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var req RequestPayload
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond([]byte(fmt.Sprintf(`{"error":"invalid request format: %v"}`, err)))
			return
		}

		snap, err := provider.CreateSnapshot(ctx, req.Entity)
		if err != nil {
			_ = msg.Respond([]byte(fmt.Sprintf(`{"error":"failed to create snapshot: %v"}`, err)))
			return
		}

		respBytes, err := json.Marshal(snap)
		if err != nil {
			_ = msg.Respond([]byte(fmt.Sprintf(`{"error":"failed to marshal snapshot: %v"}`, err)))
			return
		}

		_ = msg.Respond(respBytes)
	})

	if err != nil {
		return nil, fmt.Errorf("snapshot coordinator: failed to subscribe on %s: %w", subject, err)
	}

	return sub, nil
}

// RequestSnapshot sends a request to the cloud node to render and return a full domain snapshot.
func (c *Coordinator) RequestSnapshot(ctx context.Context, entity string, timeout time.Duration) (Snapshot, error) {
	var snap Snapshot
	nc := c.natsClient.Conn()
	if nc == nil {
		return snap, fmt.Errorf("snapshot coordinator: NATS connection is nil")
	}

	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	reqData, _ := json.Marshal(RequestPayload{Entity: entity})
	subject := c.RequestSubject()

	msg, err := nc.RequestWithContext(ctx, subject, reqData)
	if err != nil {
		return snap, fmt.Errorf("snapshot coordinator: request failed on %s: %w", subject, err)
	}

	if err := json.Unmarshal(msg.Data, &snap); err != nil {
		return snap, fmt.Errorf("snapshot coordinator: failed to unmarshal response: %w", err)
	}

	return snap, nil
}
