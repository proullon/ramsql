package ramsql

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestIn(t *testing.T) {
	log.UseTestLogger(t)

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestIn")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `SELECT * FROM user WHERE user.surname IN ('Doe', 'Simpson')`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && surname != "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 5 {
		t.Fatalf("Expected 5 rows, got %d", nb)
	}

}

func TestNotIn(t *testing.T) {
	log.UseTestLogger(t)

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestNotIn")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `SELECT * FROM user WHERE user.surname NOT IN ('Doe', 'Simpson')`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname == "Doe" || surname == "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}

}
