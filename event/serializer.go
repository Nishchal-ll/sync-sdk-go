package event

import (
	"encoding/json"
	"fmt"
)

// Serializer handles JSON encoding and decoding of EventEnvelopes.
type Serializer struct{}

// NewSerializer returns a new Serializer instance.
func NewSerializer() *Serializer {
	return &Serializer{}
}

// Marshal encodes an EventEnvelope into JSON bytes.
func (s *Serializer) Marshal(env EventEnvelope) ([]byte, error) {
	if err := env.Validate(); err != nil {
		return nil, fmt.Errorf("serializer: validation failed: %w", err)
	}
	data, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("serializer: marshal failed: %w", err)
	}
	return data, nil
}

// Unmarshal decodes JSON bytes into an EventEnvelope.
func (s *Serializer) Unmarshal(data []byte) (EventEnvelope, error) {
	var env EventEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return env, fmt.Errorf("serializer: unmarshal failed: %w", err)
	}
	if err := env.Validate(); err != nil {
		return env, fmt.Errorf("serializer: invalid envelope payload: %w", err)
	}
	return env, nil
}
