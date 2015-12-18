package ramsql

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestTransaction(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestTransaction")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE account (id INT, email TEXT)`,
		`INSERT INTO account (id, email) VALUES (1, 'foo@bar.com')`,
		`INSERT INTO account (id, email) VALUES (2, 'bar@bar.com')`,
		`CREATE TABLE champion (user_id INT, name TEXT)`,
		`INSERT INTO champion (user_id, name) VALUES (1, 'zed')`,
		`INSERT INTO champion (user_id, name) VALUES (2, 'lulu')`,
		`INSERT INTO champion (user_id, name) VALUES (1, 'thresh')`,
		`INSERT INTO champion (user_id, name) VALUES (1, 'lux')`,
	}
	for _, q := range init {
		_, err = db.Exec(q)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Cannot create tx: %s", err)
	}

	// Select count
	var count int
	err = tx.QueryRow("SELECT COUNT(user_id) FROM champion WHERE user_id = 1").Scan(&count)
	if err != nil {
		t.Fatalf("cannot query row in tx: %s\n", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 row, got %d", count)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}

	// Select count
}

func TestCheckAttributes(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestCheckAttribute")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE account (id INT, email TEXT)`,
		`INSERT INTO account (id, email) VALUES (1, 'foo@bar.com')`,
		`INSERT INTO account (id, email) VALUES (2, 'bar@bar.com')`,
	}
	for _, q := range init {
		_, err = db.Exec(q)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `INSERT INTO account(id, nonexisting_attribute) VALUES (1, foo)`
	_, err = db.Exec(query)
	if err == nil {
		t.Errorf("expected an error trying to insert non existing attribute")
	}

	query = `SELECT * FROM account WHERE nonexisting_attribute = 2`
	rows, err := db.Query(query)
	if err == nil {
		t.Errorf("expected an error trying to make a comparison with a non existing attribute")
	}

	query = `SELECT id, nonexisting_attribute FROM account WHERE id = 2`
	rows, err = db.Query(query)
	if err == nil {
		t.Errorf("expected an error trying to select a non existin attribute")
	}
	_ = rows
}
