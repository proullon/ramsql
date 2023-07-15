package engine

import (
	"testing"
)

func TestTransactionEmptyCommit(t *testing.T) {
	e := testEngine(t)
	e.Start()
	defer e.Stop()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
}

func TestTransactionEmptyRollback(t *testing.T) {
	e := testEngine(t)
	e.Start()
	defer e.Stop()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	tx.Rollback()
}
