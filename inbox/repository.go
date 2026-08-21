package inbox

import (
	"context"
)

// Transaction represents an opaque database transaction handle (e.g. *sql.Tx or *gorm.DB tx).
type Transaction interface{}

// TxInboxRepository is the interface client applications implement against their local database.
// It ensures that domain state changes and inbox ledger entries commit in the EXACT same database transaction.
type TxInboxRepository interface {
	// ExecTx executes a function inside a single local database transaction.
	// If fn returns an error or panics, the transaction is rolled back.
	ExecTx(ctx context.Context, fn func(tx Transaction) error) error

	// Exists checks if a messageID has already been processed and committed in the inbox ledger.
	Exists(ctx context.Context, tx Transaction, messageID string) (bool, error)

	// Save records a messageID into the inbox ledger inside the given transaction.
	Save(ctx context.Context, tx Transaction, messageID string, handlerName string) error
}
