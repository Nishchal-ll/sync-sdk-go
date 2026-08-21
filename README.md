<<<<<<< HEAD
# Sync SDK (`sync-sdk-go`)

> **The Golden Rule:** The Universal Sync Platform is not a data replication framework that understands application data. It is an event synchronization infrastructure that transports opaque application events reliably between authorized nodes. Business applications remain responsible for interpreting events, maintaining domain state, and resolving domain-specific conflicts.

`sync-sdk-go` is a 100% domain-agnostic, multi-tenant Go client library for building offline-first, event-driven data synchronization between POS systems, ERPs, Pharmacy management, E-Commerce platforms, mobile apps, and cloud backends powered by **NATS JetStream**.

---

## Key Features

- **Zero Domain Knowledge**: Operates exclusively on opaque `EventEnvelope` JSON payloads. No business-entity coupled code.
- **Transactional Atomicity**:
  - **Outbox Write**: Business state changes and outbox event insertions commit in the exact same local database transaction (`Tx`).
  - **Inbox Execution**: Domain handler execution and `inbox_messages` recording commit in the exact same local database transaction (`Tx`).
- **Effectively-Once Delivery**: Combines NATS `Nats-Msg-Id` windowed deduplication with permanent application-level `inbox_messages` unique constraints.
- **Multi-Node Conflict Resolution**:
  - `write_mode: single`: Cloud-authoritative replication with zero conflict overhead.
  - `write_mode: multi` (`strategy: delta`): Commutative relative adjustments for counters/stock ($\Delta -2, \Delta +5$), eliminating lost update bugs.
  - `write_mode: multi` (`strategy: state`): Causal ordering via `VectorClock` and custom `ConflictResolver` callback hooks.
- **Structural Self-Consumption Protection**: Edge nodes publish under `<node_id>` and consume only from `cloud.>`, backed by defensive SDK filtering.
- **Cold-Start & Offline Recovery**: Application-rendered snapshots via NATS request-reply proxying (`SnapshotProvider`), backed by automatic sequence checkpointing (`sync_checkpoints`) and stream replay.
- **WebSocket & Cloudflare Tunnel Ready**: Supports standard TCP (`nats://`) and encrypted WebSockets (`wss://`) for zero-port-forwarding deployments over Cloudflare Tunnels.

---

## Installation

```bash
go get github.com/biticonic/sync-sdk-go@v1.0.0
```

---

## Quickstart

```go
package main

import (
	"context"
	"log"

	"github.com/biticonic/sync-sdk-go/client"
	"github.com/biticonic/sync-sdk-go/event"
	"github.com/biticonic/sync-sdk-go/inbox"
)

func main() {
	// 1. Initialize Client
	syncClient, err := client.New(client.Config{
		AppID:    "ztt",
		TenantID: "tenant_101",
		NodeID:   "pos_01",
		NATSURL:  "wss://sync.yourdomain.com",
	})
	if err != nil {
		log.Fatalf("Failed to create sync client: %v", err)
	}

	// 2. Register Domain Message Handler
	processor := inbox.NewProcessor(myTxInboxRepo, "pos_01")
	processor.RegisterHandler("catalog.updated", func(ctx context.Context, tx inbox.Transaction, env event.EventEnvelope) error {
		// App interprets raw payload bytes
		return myCatalogService.ApplyDelta(tx, env.Payload)
	})

	// 3. Start SDK
	if err := syncClient.Start(context.Background()); err != nil {
		log.Fatalf("Failed to start sync client: %v", err)
	}
	defer syncClient.Close()
}
```

---

## Project Structure

```
sync-sdk-go/
├── client/          # Client initialization & configuration
├── conflict/        # Vector clocks, causality & ConflictResolver API
├── consumer/        # Durable JetStream pull consumer fetch loop
├── event/           # Universal EventEnvelope & JSON serializer
├── inbox/           # Transactional inbox processor & dedup ledger
├── outbox/          # Transactional outbox models & publisher worker
├── snapshot/        # SnapshotProvider & NATS request-reply coordinator
├── syncstate/       # Persistent sync_checkpoints sequence ledger
├── transport/nats/  # NATS TCP/WSS transport manager (NKEY/JWT support)
└── upcast/          # Schema payload version migration upcaster pipeline
```

---

## Running Unit Tests

```bash
go test -v ./...
```
=======
# sync-sdk-go
>>>>>>> a09ec8fc7195d317d7661b4a5e2cdf7825b8b1c8
