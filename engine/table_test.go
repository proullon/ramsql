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
	defer e.Stop()

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	_, err = e.executeQuery(i[0])
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
}

func TestInsertTable(t *testing.T) {
	query := `INSERT INTO user ('last_name', 'first_name', 'email') VALUES ('Roullon', 'Pierre', 'pierre.roullon@gmail.com')`

	e, err := New()
	if err != nil {
		t.Fatalf("Cannot create new engine: %s", err)
	}
	defer e.Stop()

	createTable(e, t)

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	_, err = e.executeQuery(i[0])
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
}

func createTable(e *Engine, t *testing.T) {
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

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	_, err = e.executeQuery(i[0])
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}

}
