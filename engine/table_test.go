package engine

import (
	"testing"

	"github.com/proullon/ramsql/engine/parser"
)

func TestCreateTable(t *testing.T) {
	query := `CREATE TABLE user
	(
        id INT PRIMARY KEY NOT NULL,
	    last_name VARCHAR(100),
	    first_name VARCHAR(100),
	    email VARCHAR(255),
	    birth_date DATE,
	    country VARCHAR(255),
	    town VARCHAR(255),
	    zip_code VARCHAR(5)
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

	if r != "table test created" {
		t.Fatalf("Query failed, expected TITIT, got %s", r)
	}
}
