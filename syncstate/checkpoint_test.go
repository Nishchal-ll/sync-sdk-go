package syncstate_test

import (
	"context"
	"sync"
	"testing"

	"github.com/biticonic/sync-sdk-go/inbox"
	"github.com/biticonic/sync-sdk-go/syncstate"
)

type MockCheckpointRepository struct {
	mu          sync.Mutex
	checkpoints map[string]syncstate.Checkpoint
}

func NewMockCheckpointRepository() *MockCheckpointRepository {
	return &MockCheckpointRepository{
		checkpoints: make(map[string]syncstate.Checkpoint),
	}
}

func (m *MockCheckpointRepository) key(consumerID, stream string) string {
	return consumerID + ":" + stream
}

func (m *MockCheckpointRepository) GetCheckpoint(ctx context.Context, consumerID, stream string) (syncstate.Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp, exists := m.checkpoints[m.key(consumerID, stream)]
	if !exists {
		return syncstate.Checkpoint{}, syncstate.ErrCheckpointNotFound
	}
	return cp, nil
}

func (m *MockCheckpointRepository) SaveCheckpoint(ctx context.Context, tx inbox.Transaction, cp syncstate.Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkpoints[m.key(cp.ConsumerID, cp.Stream)] = cp
	return nil
}

func TestCheckpointModelAndRepository(t *testing.T) {
	repo := NewMockCheckpointRepository()
	ctx := context.Background()

	consumerID := "pos_01_catalog_sub"
	stream := "ZTT_CATALOG"

	// Non-existent checkpoint returns ErrCheckpointNotFound
	_, err := repo.GetCheckpoint(ctx, consumerID, stream)
	if err != syncstate.ErrCheckpointNotFound {
		t.Fatalf("expected ErrCheckpointNotFound, got %v", err)
	}

	// Save initial checkpoint
	cp := syncstate.NewCheckpoint(consumerID, stream, 450)
	if err := repo.SaveCheckpoint(ctx, nil, cp); err != nil {
		t.Fatalf("SaveCheckpoint failed: %v", err)
	}

	// Fetch updated checkpoint
	fetched, err := repo.GetCheckpoint(ctx, consumerID, stream)
	if err != nil {
		t.Fatalf("GetCheckpoint failed: %v", err)
	}
	if fetched.Sequence != 450 {
		t.Fatalf("expected sequence 450, got %d", fetched.Sequence)
	}

	// Advance checkpoint sequence
	cp2 := syncstate.NewCheckpoint(consumerID, stream, 451)
	_ = repo.SaveCheckpoint(ctx, nil, cp2)

	fetched2, _ := repo.GetCheckpoint(ctx, consumerID, stream)
	if fetched2.Sequence != 451 {
		t.Fatalf("expected sequence 451 after advance, got %d", fetched2.Sequence)
	}
}
