package executor

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/proullon/ramsql/engine/agnostic"
)

type NamedValue struct {
	Name    string
	Ordinal int
	Value   any
}

type Tx struct {
	e  *Engine
	tx *agnostic.Transaction
}

func NewTx(ctx context.Context, e *Engine, opts sql.TxOptions) (*Tx, error) {
	tx, err := e.memstore.Begin()
	if err != nil {
		return nil, err
	}

	t := &Tx{
		e:  e,
		tx: tx,
	}

	return t, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args []NamedValue) (string, error) {
	return "", nil
}

// Commit the transaction on server
func (t *Tx) Commit() error {
	_, err := t.tx.Commit()
	return err
}

// Rollback all changes
func (t *Tx) Rollback() error {
	t.tx.Rollback()
	return nil
}

func (t *Tx) ExecContext(ctx context.Context, query string, args []NamedValue) (int64, int64, error) {
	return 0, 0, fmt.Errorf("not implemented")
}
