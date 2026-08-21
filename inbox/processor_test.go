package inbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/biticonic/sync-sdk-go/event"
	"github.com/biticonic/sync-sdk-go/inbox"
)

type MockTx struct{}

type MockTxInboxRepository struct {
	mu     sync.Mutex
	ledger map[string]string // messageID -> handlerName
}

func NewMockTxInboxRepository() *MockTxInboxRepository {
	return &MockTxInboxRepository{
		ledger: make(map[string]string),
	}
}

func (m *MockTxInboxRepository) ExecTx(ctx context.Context, fn func(tx inbox.Transaction) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	mockTx := &MockTx{}
	return fn(mockTx)
}

func (m *MockTxInboxRepository) Exists(ctx context.Context, tx inbox.Transaction, messageID string) (bool, error) {
	_, exists := m.ledger[messageID]
	return exists, nil
}

func (m *MockTxInboxRepository) Save(ctx context.Context, tx inbox.Transaction, messageID string, handlerName string) error {
	m.ledger[messageID] = handlerName
	return nil
}

func TestInboxProcessorAtomicExecution(t *testing.T) {
	repo := NewMockTxInboxRepository()
	processor := inbox.NewProcessor(repo, "pos_01")
	ctx := context.Background()

	handlerRanCount := 0
	processor.RegisterHandler("product.updated", func(ctx context.Context, tx inbox.Transaction, env event.EventEnvelope) error {
		handlerRanCount++
		return nil
	})

	serializer := event.NewSerializer()
	env := event.NewEnvelope("ztt", "cloud_main", "product", "PROD-100", "updated", []byte(`{"name":"Coffee"}`))
	data, _ := serializer.Marshal(env)

	// First execution -> Handler runs, Inbox row saved
	if err := processor.ProcessMessage(ctx, data); err != nil {
		t.Fatalf("first ProcessMessage failed: %v", err)
	}
	if handlerRanCount != 1 {
		t.Fatalf("expected handler to run once, got %d", handlerRanCount)
	}

	exists, _ := repo.Exists(ctx, nil, env.ID)
	if !exists {
		t.Fatal("expected message ID to exist in ledger")
	}

	// Second execution (duplicate payload) -> Handler skipped, no error
	if err := processor.ProcessMessage(ctx, data); err != nil {
		t.Fatalf("duplicate ProcessMessage failed: %v", err)
	}
	if handlerRanCount != 1 {
		t.Fatalf("expected handler to STILL run only once on duplicate, got %d", handlerRanCount)
	}
}

func TestInboxProcessorHandlerErrorRollback(t *testing.T) {
	repo := NewMockTxInboxRepository()
	processor := inbox.NewProcessor(repo, "pos_01")
	ctx := context.Background()

	errDummy := errors.New("database update failed")
	processor.RegisterHandler("product.updated", func(ctx context.Context, tx inbox.Transaction, env event.EventEnvelope) error {
		return errDummy
	})

	serializer := event.NewSerializer()
	env := event.NewEnvelope("ztt", "cloud_main", "product", "PROD-100", "updated", []byte(`{}`))
	data, _ := serializer.Marshal(env)

	err := processor.ProcessMessage(ctx, data)
	if err == nil {
		t.Fatal("expected processor error when handler fails")
	}

	exists, _ := repo.Exists(ctx, nil, env.ID)
	if exists {
		t.Fatal("inbox ledger should NOT contain message ID when handler fails")
	}
}

func TestInboxProcessorSelfConsumptionFilter(t *testing.T) {
	repo := NewMockTxInboxRepository()
	clientNodeID := "pos_01"
	processor := inbox.NewProcessor(repo, clientNodeID)
	ctx := context.Background()

	handlerRan := false
	processor.RegisterHandler("sales.created", func(ctx context.Context, tx inbox.Transaction, env event.EventEnvelope) error {
		handlerRan = true
		return nil
	})

	serializer := event.NewSerializer()
	// Envelope published by the SAME node (pos_01)
	selfEnv := event.NewEnvelope("ztt", clientNodeID, "sales", "SALE-999", "created", []byte(`{}`))
	data, _ := serializer.Marshal(selfEnv)

	if err := processor.ProcessMessage(ctx, data); err != nil {
		t.Fatalf("ProcessMessage failed: %v", err)
	}

	if handlerRan {
		t.Fatal("handler should NOT run for self-published events")
	}
}
