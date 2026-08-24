package client

import (
	"errors"
	"time"

	"github.com/biticonic/sync-sdk-go/transport/nats"
)

var (
	ErrMissingConfigAppID  = errors.New("client: config missing AppID")
	ErrMissingConfigNodeID = errors.New("client: config missing NodeID")
)

// Config holds initialization options for the Sync SDK client.
type Config struct {
	AppID          string        // Application Identifier (e.g., "ztt")
	TenantID       string        // Tenant Identifier (e.g., "tenant_101")
	NodeID         string        // Unique Node Identifier (e.g., "01cc92a6-...", "cloud")
	NATSURL        string        // NATS Server URL (e.g., "nats://localhost:4222" or "wss://sync.domain.com")
	UserCredsFile  string        // Path to NATS credentials file for NKEY auth
	ConnectTimeout time.Duration // Connection timeout duration
	InsecureSkipVerify bool          // Skip TLS verification for dev/WSS
}

// Validate checks that mandatory client configuration fields are set.
func (c Config) Validate() error {
	if c.AppID == "" {
		return ErrMissingConfigAppID
	}
	if c.TenantID == "" {
		return errors.New("client: config missing TenantID")
	}
	if c.NodeID == "" {
		return ErrMissingConfigNodeID
	}
	return nil
}

// NATSConfig converts client config into nats.Config.
func (c Config) NATSConfig() nats.Config {
	return nats.Config{
		URL:            c.NATSURL,
		UserCredsFile:  c.UserCredsFile,
		ConnectTimeout: c.ConnectTimeout,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}
}
