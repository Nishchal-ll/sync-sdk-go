package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/biticonic/sync-sdk-go/transport/nats"
)

// Client is the main SDK entrypoint managing connections and background sync workers.
type Client struct {
	cfg        Config
	natsClient *nats.Client
	mu         sync.RWMutex
	running    bool
}

// New initializes a new SDK Client instance.
func New(cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("syncsdk: invalid configuration: %w", err)
	}

	natsClient, err := nats.NewClient(cfg.NATSConfig())
	if err != nil {
		return nil, fmt.Errorf("syncsdk: failed to initialize NATS transport: %w", err)
	}

	return &Client{
		cfg:        cfg,
		natsClient: natsClient,
	}, nil
}

// AppID returns the configured application identifier.
func (c *Client) AppID() string {
	return c.cfg.AppID
}

// NodeID returns the configured node identifier.
func (c *Client) NodeID() string {
	return c.cfg.NodeID
}

// Transport returns the underlying NATS transport client.
func (c *Client) Transport() *nats.Client {
	return c.natsClient
}

// Start begins background workers and consumer processing loops.
func (c *Client) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}
	c.running = true

	return nil
}

// Close gracefully stops workers and closes the NATS connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}
	c.running = false

	if c.natsClient != nil {
		c.natsClient.Close()
	}
	return nil
}
