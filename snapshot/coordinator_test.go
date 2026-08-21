package snapshot_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/biticonic/sync-sdk-go/snapshot"
)

type MockSnapshotProvider struct {
	snapshots map[string]snapshot.Snapshot
}

func NewMockSnapshotProvider() *MockSnapshotProvider {
	return &MockSnapshotProvider{
		snapshots: make(map[string]snapshot.Snapshot),
	}
}

func (m *MockSnapshotProvider) CreateSnapshot(ctx context.Context, entity string) (snapshot.Snapshot, error) {
	snap, exists := m.snapshots[entity]
	if !exists {
		data := json.RawMessage(`{"items": [{"id": "P1", "name": "Item 1"}]}`)
		return snapshot.NewSnapshot(entity, data, 1000), nil
	}
	return snap, nil
}

func (m *MockSnapshotProvider) RestoreSnapshot(ctx context.Context, snap snapshot.Snapshot) error {
	m.snapshots[snap.Entity] = snap
	return nil
}

func TestSnapshotModelAndProvider(t *testing.T) {
	provider := NewMockSnapshotProvider()
	ctx := context.Background()

	snap, err := provider.CreateSnapshot(ctx, "catalog")
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	if snap.Entity != "catalog" {
		t.Fatalf("expected entity 'catalog', got '%s'", snap.Entity)
	}
	if snap.StreamSeq != 1000 {
		t.Fatalf("expected stream_seq 1000, got %d", snap.StreamSeq)
	}

	if err := provider.RestoreSnapshot(ctx, snap); err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}

	restored, _ := provider.CreateSnapshot(ctx, "catalog")
	if restored.StreamSeq != 1000 {
		t.Fatalf("expected restored stream_seq 1000, got %d", restored.StreamSeq)
	}
}
