package inbox

import (
	"context"
	"fmt"
	"sync"

	"github.com/biticonic/sync-sdk-go/conflict"
	"github.com/biticonic/sync-sdk-go/event"
)

// HandlerFunc is the function signature for domain message handlers.
type HandlerFunc func(ctx context.Context, tx Transaction, env event.EventEnvelope) error

// Processor manages domain handler registrations, conflict resolvers, and atomic transactional inbox execution.
type Processor struct {
	repo              TxInboxRepository
	serializer        *event.Serializer
	clientNodeID      string
	mu                sync.RWMutex
	handlers          map[string]HandlerFunc
	conflictResolvers map[string]conflict.ConflictResolver
}

// NewProcessor initializes a new inbox Processor.
func NewProcessor(repo TxInboxRepository, clientNodeID string) *Processor {
	return &Processor{
		repo:              repo,
		serializer:        event.NewSerializer(),
		clientNodeID:      clientNodeID,
		handlers:          make(map[string]HandlerFunc),
		conflictResolvers: make(map[string]conflict.ConflictResolver),
	}
}

// RegisterHandler registers a domain handler for a specific entity or action pattern (e.g. "product", "order.created").
func (p *Processor) RegisterHandler(key string, handler HandlerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlers[key] = handler
}

// RegisterConflictResolver registers a custom ConflictResolver for an entity.
func (p *Processor) RegisterConflictResolver(entity string, resolver conflict.ConflictResolver) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conflictResolvers[entity] = resolver
}

// ProcessMessage deserializes an envelope, checks for self-consumption, and atomically executes handler + inbox write.
func (p *Processor) ProcessMessage(ctx context.Context, rawData []byte) error {
	env, err := p.serializer.Unmarshal(rawData)
	if err != nil {
		return fmt.Errorf("inbox: invalid envelope payload: %w", err)
	}

	if env.NodeID == p.clientNodeID {
		return nil
	}

	p.mu.RLock()
	handlerKey := fmt.Sprintf("%s.%s", env.Entity, env.Action)
	handler, exists := p.handlers[handlerKey]
	if !exists {
		handler, exists = p.handlers[env.Entity]
	}
	resolver := p.conflictResolvers[env.Entity]
	p.mu.RUnlock()

	if !exists {
		return fmt.Errorf("inbox: no registered handler for entity key '%s'", handlerKey)
	}

	return p.repo.ExecTx(ctx, func(tx Transaction) error {
		alreadyProcessed, err := p.repo.Exists(ctx, tx, env.ID)
		if err != nil {
			return fmt.Errorf("inbox: failed to check message existence: %w", err)
		}
		if alreadyProcessed {
			return nil
		}

		targetEnv := env
		_ = resolver

		if err := handler(ctx, tx, targetEnv); err != nil {
			return fmt.Errorf("inbox: domain handler failed: %w", err)
		}

		if err := p.repo.Save(ctx, tx, targetEnv.ID, handlerKey); err != nil {
			return fmt.Errorf("inbox: failed to save inbox record: %w", err)
		}

		return nil
	})
}
