package agnostic

import (
	"reflect"
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

	attrs := []Attribute{
		Attribute{
			name:     "foo",
			typeName: "BIGINT",
		},
		Attribute{
			name:     "bar",
			typeName: "TEXT",
		},
	}

	err = tx.CreateRelation("", "myrel", attrs)
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

	attrs := []Attribute{
		Attribute{
			name:     "foo",
			typeName: "BIGINT",
		},
		Attribute{
			name:     "bar",
			typeName: "TEXT",
		},
	}

	err = tx.CreateRelation("", "myrel", attrs)
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

func TestInsertTotal(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("foo", "BIGINT"),
		NewAttribute("bar", "TEXT"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["bar"] = "test"
	tuple, err := tx.Insert(schema, relation, values)
	if err == nil {
		t.Fatalf("expected error with foo attribute not specified")
	}

	tuple, err = tx.Insert(schema, relation, values)
	if err == nil {
		t.Fatalf("expected transaction aborted due to previous error")
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values["foo"] = 1
	tuple, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	l := len(tuple.values)
	if l != 2 {
		t.Fatalf("expected 2 values in tuple, got %d", l)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	l = e.schemas[schema].relations[relation].rows.Len()
	if l != 1 {
		t.Fatalf("expected 1 rows in relation, got %d", l)
	}
}

func TestInsertRollback(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("foo", "BIGINT"),
		NewAttribute("bar", "TEXT"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["bar"] = "test"
	values["foo"] = 1
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	tx.Rollback()

	l := e.schemas[schema].relations[relation].rows.Len()
	if l != 0 {
		t.Fatalf("expected 0 rows in relation, got %d", l)
	}
}

func TestInsertPartial(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("default_answer", "INT").WithDefault(42),
		NewAttribute("foo", "JSON"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["foo"] = `{}`
	tuple, err := tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	l := len(tuple.values)
	if l != 3 {
		t.Fatalf("expected 3 values in tuple, got %d", l)
	}
	if _, ok := tuple.values[0].(int64); !ok {
		t.Fatalf("expected 1st tuple value to be an int64, got %s", reflect.TypeOf(tuple.values[0]))
	}
	if val, _ := tuple.values[0].(int64); val != 1 {
		t.Fatalf("expected 1st tuple value to be 1, got %d", val)
	}
	if _, ok := tuple.values[1].(int64); !ok {
		t.Fatalf("expected 2nd tuple value to be an int64, got %s", reflect.TypeOf(tuple.values[1]))
	}
	if val, _ := tuple.values[1].(int64); val != 42 {
		t.Fatalf("expected 2nd tuple value to be 1, got %d", val)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	l = e.schemas[schema].relations[relation].rows.Len()
	if l != 1 {
		t.Fatalf("expected 1 rows in relation, got %d", l)
	}
}
