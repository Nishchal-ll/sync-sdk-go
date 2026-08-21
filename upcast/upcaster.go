package upcast

import (
	"encoding/json"
	"fmt"
	"sync"
)

// UpcasterFunc is the signature for payload migration functions transforming version A to version B.
type UpcasterFunc func(rawPayload json.RawMessage) (json.RawMessage, error)

// Registry manages schema version migration pipelines for entities.
type Registry struct {
	mu        sync.RWMutex
	upcasters map[string]UpcasterFunc // key: "entity:fromVersion->toVersion"
}

// NewRegistry creates a new Upcaster Registry.
func NewRegistry() *Registry {
	return &Registry{
		upcasters: make(map[string]UpcasterFunc),
	}
}

func key(entity, fromVersion, toVersion string) string {
	return fmt.Sprintf("%s:%s->%s", entity, fromVersion, toVersion)
}

// Register adds a migration function to transform an entity's payload from one version to another.
func (r *Registry) Register(entity, fromVersion, toVersion string, fn UpcasterFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upcasters[key(entity, fromVersion, toVersion)] = fn
}

// Upcast transforms a payload from sourceVersion to targetVersion using registered migration functions.
func (r *Registry) Upcast(entity, sourceVersion, targetVersion string, rawPayload json.RawMessage) (json.RawMessage, error) {
	if sourceVersion == targetVersion {
		return rawPayload, nil
	}

	r.mu.RLock()
	fn, exists := r.upcasters[key(entity, sourceVersion, targetVersion)]
	r.mu.RUnlock()

	if !exists {
		return rawPayload, fmt.Errorf("upcast: no migration registered for %s from %s to %s", entity, sourceVersion, targetVersion)
	}

	migrated, err := fn(rawPayload)
	if err != nil {
		return nil, fmt.Errorf("upcast: migration failed for %s (%s->%s): %w", entity, sourceVersion, targetVersion, err)
	}

	return migrated, nil
}
