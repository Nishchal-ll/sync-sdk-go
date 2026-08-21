package conflict

// Relation represents the causal ordering relationship between two vector clocks.
type Relation int

const (
	Equal       Relation = iota // Clock A == Clock B
	Dominates                   // Clock A strictly supersedes Clock B (A > B)
	Subordinate                 // Clock A is strictly superseded by Clock B (A < B)
	Concurrent                  // Clock A and Clock B diverged concurrently (A || B)
)

// VectorClock represents a mapping of node IDs to monotonic sequence numbers.
type VectorClock map[string]int64

// Compare determines the causal relationship between clockA (local) and clockB (incoming).
func Compare(clockA, clockB VectorClock) Relation {
	if len(clockA) == 0 && len(clockB) == 0 {
		return Equal
	}
	if len(clockA) == 0 && len(clockB) > 0 {
		return Subordinate
	}
	if len(clockA) > 0 && len(clockB) == 0 {
		return Dominates
	}

	greater := false
	lesser := false

	// Collect all keys from both clocks
	allKeys := make(map[string]bool)
	for k := range clockA {
		allKeys[k] = true
	}
	for k := range clockB {
		allKeys[k] = true
	}

	for k := range allKeys {
		vA := clockA[k]
		vB := clockB[k]

		if vA > vB {
			greater = true
		}
		if vA < vB {
			lesser = true
		}
	}

	if greater && lesser {
		return Concurrent
	}
	if greater {
		return Dominates
	}
	if lesser {
		return Subordinate
	}
	return Equal
}

// Increment returns a new vector clock with the given nodeID sequence incremented by 1.
func Increment(clock VectorClock, nodeID string) VectorClock {
	newClock := make(VectorClock, len(clock)+1)
	for k, v := range clock {
		newClock[k] = v
	}
	newClock[nodeID]++
	return newClock
}

// MergeClocks combines two vector clocks taking the maximum value for each node ID key.
func MergeClocks(clockA, clockB VectorClock) VectorClock {
	merged := make(VectorClock)
	for k, v := range clockA {
		merged[k] = v
	}
	for k, v := range clockB {
		if v > merged[k] {
			merged[k] = v
		}
	}
	return merged
}
