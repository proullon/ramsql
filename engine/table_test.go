package engine

import (
	"testing"

	"github.com/proullon/ramsql/engine/parser"
)

func TestCreateTable(t *testing.T) {
	query := `CREATE TABLE user
	(
        id INT PRIMARY KEY,
	    last_name TEXT,
	    first_name TEXT,
	    email TEXT,
	    birth_date DATE,
	    country TEXT,
	    town TEXT,
	    zip_code TEXT
	)`

	e, err := New()
	if err != nil {
		t.Fatalf("Cannot create new engine: %s", err)
	}

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	r, err := e.executeQuery(i[0])
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}

	expected := "table user created"
	if r != expected {
		t.Fatalf("Query failed, expected %s, got %s", expected, r)
	}
}
