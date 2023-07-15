package agnostic

import (
	"testing"
)

func TestTransactionEmptyCommit(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 0 {
		t.Fatalf("expected no line changed, got %d", changed)
	}
}

func TestTransactionEmptyRollback(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	tx.Rollback()
	tx.Rollback()
}

func TestCreateRelation(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	err = tx.CreateRelation("", "myrel")
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	if len(e.schemas[DefaultSchema].relations) != 1 {
		t.Fatalf("expected a relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	tx.Rollback()
}

func TestDropRelation(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	err = tx.CreateRelation("", "myrel")
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	err = tx.DropRelation("", "myrel")
	if err != nil {
		t.Fatalf("cannot drop relation: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 2 {
		t.Fatalf("expected 2 change, got %d", changed)
	}

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}
}
