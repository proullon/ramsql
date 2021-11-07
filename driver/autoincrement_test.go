package ramsql

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestAutoIncrementSimple(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestAutoIncrementSimple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	res, err := db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot fetch last inserted id: %s\n", err)
	}

	if lastID != 1 {
		t.Fatalf("Last insterted id should be 1, not %d", lastID)
	}

	res, err = db.Exec("INSERT INTO account ('email') VALUES ('roger@gmail.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	lastID, err = res.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot fetch last inserted id: %s\n", err)
	}

	if lastID != 2 {
		t.Fatalf("Last insterted id should be 2, not %d", lastID)
	}
}

func TestAutoIncrementAlternativeSimple(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestAutoIncrementAlternativeSimple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTO_INCREMENT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	res, err := db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	lastID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot fetch last inserted id: %s\n", err)
	}

	if lastID != 1 {
		t.Fatalf("Last insterted id should be 1, not %d", lastID)
	}

	res, err = db.Exec("INSERT INTO account ('email') VALUES ('roger@gmail.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	lastID, err = res.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot fetch last inserted id: %s\n", err)
	}

	if lastID != 2 {
		t.Fatalf("Last insterted id should be 2, not %d", lastID)
	}
}
