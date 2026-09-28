package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/biticonic/sync-sdk-go/inbox"
	"github.com/biticonic/sync-sdk-go/syncstate"
	natsTransport "github.com/biticonic/sync-sdk-go/transport/nats"
	"github.com/nats-io/nats.go"
)

// Config defines settings for the NATS JetStream durable pull consumer.
type Config struct {
	StreamName     string                      // NATS JetStream stream name (e.g. "ZTT_CATALOG")
	DurableName    string                      // Consumer durable name (e.g. "pos_01_catalog_sub")
	FilterSubject  string                      // Subject filter pattern (e.g. "ztt.tenant_101.cloud.catalog.>")
	FetchBatch     int                         // Pull batch size (default 10)
	FetchWait      time.Duration               // Pull fetch timeout (default 2s)
	StartSeq       uint64                      // Explicit start sequence override (0 uses checkpoint or default)
	CheckpointRepo syncstate.CheckpointRepository // Optional checkpoint storage for resume/replay
}

// Consumer handles durable message fetching from NATS JetStream and dispatches to inbox.Processor.
type Consumer struct {
	natsClient     *natsTransport.Client
	processor      *inbox.Processor
	checkpointRepo syncstate.CheckpointRepository
	cfg            Config
	sub            *nats.Subscription
	mu             sync.Mutex
	running        bool
	stopChan       chan struct{}
}

// NewConsumer initializes a new durable pull Consumer instance.
func NewConsumer(natsClient *natsTransport.Client, processor *inbox.Processor, cfg Config) *Consumer {
	if cfg.FetchBatch <= 0 {
		cfg.FetchBatch = 10
	}
	if cfg.FetchWait <= 0 {
		cfg.FetchWait = 2 * time.Second
	}

	return &Consumer{
		natsClient:     natsClient,
		processor:      processor,
		checkpointRepo: cfg.CheckpointRepo,
		cfg:            cfg,
		stopChan:       make(chan struct{}),
	}
}

// Start creates the NATS durable subscription and starts the fetch loop.
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	js := c.natsClient.JetStream()
	if js == nil {
		return fmt.Errorf("consumer: jetstream context is nil")
	}

	var subOpts []nats.SubOpt

	// Check checkpoint for resume-from-sequence logic
	startSeq := c.cfg.StartSeq
	if startSeq == 0 && c.checkpointRepo != nil {
		cp, err := c.checkpointRepo.GetCheckpoint(ctx, c.cfg.DurableName, c.cfg.StreamName)
		if err == nil && cp.Sequence > 0 {
			startSeq = cp.Sequence + 1
		}
	}

	if startSeq > 0 {
		subOpts = append(subOpts, nats.StartSequence(startSeq))
	}

	subOpts = append(subOpts, nats.Bind(c.cfg.StreamName, c.cfg.DurableName))

	sub, err := js.PullSubscribe(
		c.cfg.FilterSubject,
		c.cfg.DurableName,
		subOpts...,
	)
	if err != nil {
		// Fallback: Subscribe without strict Bind if creating on demand
		fallbackOpts := []nats.SubOpt{}
		if startSeq > 0 {
			fallbackOpts = append(fallbackOpts, nats.StartSequence(startSeq))
		}
		sub, err = js.PullSubscribe(c.cfg.FilterSubject, c.cfg.DurableName, fallbackOpts...)
		if err != nil {
			return fmt.Errorf("consumer: pull subscribe failed on %s: %w", c.cfg.FilterSubject, err)
		}
	}

	c.sub = sub
	c.running = true

	go c.loop(ctx)
	return nil
}

// Stop gracefully stops the consumer loop and unsubscribes.
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return
	}
	c.running = false
	close(c.stopChan)

	if c.sub != nil {
		_ = c.sub.Unsubscribe()
	}
}

func (c *Consumer) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopChan:
			return
		default:
			msgs, err := c.sub.Fetch(c.cfg.FetchBatch, nats.MaxWait(c.cfg.FetchWait))
			if err != nil {
				if err == nats.ErrTimeout {
					continue
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, msg := range msgs {
				if ctx.Err() != nil {
					break
				}
				c.handleMsg(ctx, msg)
			}
		}
	}
}

func (c *Consumer) handleMsg(ctx context.Context, msg *nats.Msg) {
	err := c.processor.ProcessMessage(ctx, msg.Data)
	if err != nil {
		// Handler error or rollback -> NAK message for JetStream retry
		_ = msg.Nak()
		return
	}

	// Successfully processed or deduplicated -> Update checkpoint if sequence metadata exists
	meta, metaErr := msg.Metadata()
	if metaErr == nil && c.checkpointRepo != nil && meta != nil {
		cp := syncstate.NewCheckpoint(c.cfg.DurableName, c.cfg.StreamName, meta.Sequence.Stream)
		_ = c.checkpointRepo.SaveCheckpoint(ctx, nil, cp)
	}

	_ = msg.Ack()
}
