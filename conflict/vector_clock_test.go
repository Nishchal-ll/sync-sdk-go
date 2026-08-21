package conflict_test

import (
	"context"
	"testing"

	"github.com/biticonic/sync-sdk-go/conflict"
	"github.com/biticonic/sync-sdk-go/event"
)

func TestVectorClockComparison(t *testing.T) {
	tests := []struct {
		name     string
		clockA   conflict.VectorClock
		clockB   conflict.VectorClock
		expected conflict.Relation
	}{
		{
			name:     "Both empty clocks",
			clockA:   conflict.VectorClock{},
			clockB:   conflict.VectorClock{},
			expected: conflict.Equal,
		},
		{
			name:     "Clock A dominates Clock B",
			clockA:   conflict.VectorClock{"node1": 2, "node2": 1},
			clockB:   conflict.VectorClock{"node1": 1, "node2": 1},
			expected: conflict.Dominates,
		},
		{
			name:     "Clock A subordinate to Clock B",
			clockA:   conflict.VectorClock{"node1": 1, "node2": 1},
			clockB:   conflict.VectorClock{"node1": 2, "node2": 2},
			expected: conflict.Subordinate,
		},
		{
			name:     "Concurrent divergence",
			clockA:   conflict.VectorClock{"node1": 2, "node2": 1},
			clockB:   conflict.VectorClock{"node1": 1, "node2": 2},
			expected: conflict.Concurrent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := conflict.Compare(tt.clockA, tt.clockB)
			if rel != tt.expected {
				t.Fatalf("expected relation %v, got %v", tt.expected, rel)
			}
		})
	}
}

func TestVectorClockIncrementAndMerge(t *testing.T) {
	clockA := conflict.VectorClock{"node1": 1, "node2": 3}
	incClock := conflict.Increment(clockA, "node1")

	if incClock["node1"] != 2 {
		t.Fatalf("expected incremented sequence 2, got %d", incClock["node1"])
	}

	clockB := conflict.VectorClock{"node1": 2, "node2": 1, "node3": 5}
	merged := conflict.MergeClocks(clockA, clockB)

	if merged["node1"] != 2 || merged["node2"] != 3 || merged["node3"] != 5 {
		t.Fatalf("unexpected merged clock result: %+v", merged)
	}
}

func TestConflictResolverCallback(t *testing.T) {
	ctx := context.Background()

	localEnv := event.NewEnvelope("ztt", "t1", "node1", "product", "P1", "updated", []byte(`{"name":"Local"}`))
	incomingEnv := event.NewEnvelope("ztt", "t1", "node2", "product", "P1", "updated", []byte(`{"name":"Remote"}`))

	resolver := conflict.ConflictResolverFunc(func(ctx context.Context, local, incoming event.EventEnvelope) (conflict.Resolution, *event.EventEnvelope, error) {
		mergedPayload := []byte(`{"name":"Merged (Local+Remote)"}`)
		mergedEnv := local
		mergedEnv.Payload = mergedPayload
		return conflict.Merge, &mergedEnv, nil
	})

	res, mergedEnv, err := resolver.Resolve(ctx, localEnv, incomingEnv)
	if err != nil {
		t.Fatalf("resolver failed: %v", err)
	}
	if res != conflict.Merge {
		t.Fatalf("expected resolution Merge, got %v", res)
	}
	if string(mergedEnv.Payload) != `{"name":"Merged (Local+Remote)"}` {
		t.Fatalf("unexpected merged payload: %s", string(mergedEnv.Payload))
	}
}
