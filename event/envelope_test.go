package event_test

import (
	"testing"

	"github.com/biticonic/sync-sdk-go/event"
)

func TestNewEnvelopeAndValidation(t *testing.T) {
	payload := []byte(`{"price": 100.50, "sku": "SKU-999"}`)
	env := event.NewEnvelope("ztt", "tenant_101", "pos_01", "product", "SKU-999", "updated", payload)

	if env.ID == "" {
		t.Fatal("expected non-empty UUID for EventEnvelope.ID")
	}
	if env.Version != "1.0" {
		t.Fatalf("expected version '1.0', got '%s'", env.Version)
	}

	if err := env.Validate(); err != nil {
		t.Fatalf("validation failed unexpectedly: %v", err)
	}

	expectedSubject := "ztt.tenant_101.pos_01.product.updated"
	if subject := env.Subject(); subject != expectedSubject {
		t.Fatalf("expected subject '%s', got '%s'", expectedSubject, subject)
	}
}

func TestEnvelopeValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		env     event.EventEnvelope
		wantErr error
	}{
		{
			name:    "Missing AppID",
			env:     event.EventEnvelope{TenantID: "t1", NodeID: "n1", Entity: "e1", EntityID: "id1", Action: "a1"},
			wantErr: event.ErrMissingAppID,
		},
		{
			name:    "Missing TenantID",
			env:     event.EventEnvelope{AppID: "app1", NodeID: "n1", Entity: "e1", EntityID: "id1", Action: "a1"},
			wantErr: event.ErrMissingTenantID,
		},
		{
			name:    "Missing NodeID",
			env:     event.EventEnvelope{AppID: "app1", TenantID: "t1", Entity: "e1", EntityID: "id1", Action: "a1"},
			wantErr: event.ErrMissingNodeID,
		},
		{
			name:    "Missing Entity",
			env:     event.EventEnvelope{AppID: "app1", TenantID: "t1", NodeID: "n1", EntityID: "id1", Action: "a1"},
			wantErr: event.ErrMissingEntity,
		},
		{
			name:    "Missing EntityID",
			env:     event.EventEnvelope{AppID: "app1", TenantID: "t1", NodeID: "n1", Entity: "e1", Action: "a1"},
			wantErr: event.ErrMissingEntityID,
		},
		{
			name:    "Missing Action",
			env:     event.EventEnvelope{AppID: "app1", TenantID: "t1", NodeID: "n1", Entity: "e1", EntityID: "id1"},
			wantErr: event.ErrMissingAction,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if err != tt.wantErr {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestSerializerFidelity(t *testing.T) {
	serializer := event.NewSerializer()
	payload := []byte(`{"quantity": 42}`)
	originalEnv := event.NewEnvelope("ztt", "tenant_101", "pos_01", "inventory", "INV-001", "adjusted", payload)

	data, err := serializer.Marshal(originalEnv)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	decodedEnv, err := serializer.Unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if decodedEnv.ID != originalEnv.ID {
		t.Fatalf("ID mismatch: got %s, want %s", decodedEnv.ID, originalEnv.ID)
	}
	if decodedEnv.Subject() != originalEnv.Subject() {
		t.Fatalf("Subject mismatch: got %s, want %s", decodedEnv.Subject(), originalEnv.Subject())
	}
}
