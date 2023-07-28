package engine

/*
import (
	"testing"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

func TestCreateRelation(t *testing.T) {
	e := testEngine(t)
	e.Start()
	defer e.Stop()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	relationName := "user"
	schemaName := "public"
	err = tx.CreateRelation(schemaName, relationName)
	if err != nil {
		t.Fatalf("cannot create table '%s'.'%s': %s", schemaName, relationName, err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
}

func TestCreateTableSQL(t *testing.T) {
	log.UseTestLogger(t)
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

	e := testEngine(t)
	defer e.Stop()

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	err = e.executeQuery(i[0], &TestEngineConn{})
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
}

func TestInsertTable(t *testing.T) {
	log.UseTestLogger(t)
	query := `INSERT INTO user ('last_name', 'first_name', 'email') VALUES ('Roullon', 'Pierre', 'pierre.roullon@gmail.com')`

	e := testEngine(t)
	defer e.Stop()

	createTable(e, t)

	i, err := parser.ParseInstruction(query)
	if err != nil {
		t.Fatalf("Cannot parse query %s : %s", query, err)
	}

	err = e.executeQuery(i[0], &TestEngineConn{})
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}
}

func createTable(e *Engine, t *testing.T) {
	log.UseTestLogger(t)
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

	err = e.executeQuery(i[0], &TestEngineConn{})
	if err != nil {
		t.Fatalf("Cannot execute query: %s", err)
	}

}
*/
