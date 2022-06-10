package engine_test

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func TestInsertSingle(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestInsertASingle")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Uhura')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("Expected to affect 1 row, affected %v", rowsAffected)
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot check last inserted ID: %s", err)
	}

	row := db.QueryRow("SELECT breed, name FROM cat WHERE id = ?", insertedId)
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var breed string
	var name string
	err = row.Scan(&breed, &name)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if breed != "indeterminate" || name != "Uhura" {
		t.Fatalf("Expected breed 'indeterminate' and name 'Uhura', got breed '%v' and name '%v'", breed, name)
	}
}

func TestInsertSingleReturning(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestInsertSingleReturning")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	rows, err := db.Query("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Nala') RETURNING id")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}
	defer rows.Close()

	hasRow := rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id 1, got id %v", id)
	}

	hasRow = rows.Next()
	if hasRow {
		t.Fatalf("Returned more than one row: %s", err)
	}
}

func TestInsertWithMissingValue(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestInsertWithMissingValue")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE user (
			id INT AUTOINCREMENT,
			email TEXT DEFAULT 'example@example.com',
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO user (name) VALUES ('Bob')")
	if err != nil {
		t.Fatalf("Cannot insert into table user: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("Expected to affect 1 row, affected %v", rowsAffected)
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot check last inserted ID: %s", err)
	}

	row := db.QueryRow("SELECT email, name FROM user WHERE id = ?", insertedId)
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var email string
	var name string
	err = row.Scan(&email, &name)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if email != "example@example.com" || name != "Bob" {
		t.Fatalf("Expected email 'example@example.com' and name 'Bob', got email '%v' and name '%v'", email, name)
	}
}

func TestInsertMultiple(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestInsertMultiple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO cat (breed, name) VALUES ('persian', 'Mozart'), ('persian', 'Danton')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 2 {
		t.Fatalf("Expected to affect 2 rows, affected %v", rowsAffected)
	}
}

func TestInsertMultipleReturning(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestInsertMultipleReturning")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	rows, err := db.Query("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Spock'), ('indeterminate', 'Belanna') RETURNING id")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}
	defer rows.Close()

	hasRow := rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if id != 1 {
		t.Fatalf("Expected id 1, got id %v", id)
	}

	hasRow = rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if id != 2 {
		t.Fatalf("Expected id 2, got id %v", id)
	}

	hasRow = rows.Next()
	if hasRow {
		t.Fatalf("Returned more than two rows: %s", err)
	}
}
