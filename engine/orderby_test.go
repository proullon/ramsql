package engine_test

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"
)

func TestOrderByInt(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestOrderByInt")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

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

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE surname = Wayne OR surname = Doe ORDER BY age DESC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}
	defer rows.Close()

	var age, last, size int64
	last = 4000
	// Now every time 'age' should be less than 'last'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if age > last {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 4 {
		t.Fatalf("Expecting 4 rows here, got %d", size)
	}

	query = `SELECT age FROM user ORDER BY age ASC`
	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("cannot order by age: %s\n", err)
	}

	size = 0
	last = 0
	// Now 'last' should be less than current 'age'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if last > age {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 7 {
		t.Fatalf("Expecting 7 rows, got %d", size)
	}
}

func TestOrderByString(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestOrderByString")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

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

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT name, surname FROM user WHERE surname = $1 ORDER BY name ASC`
	rows, err := db.Query(query, "Doe")
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}

	var names []string
	var name, surname string
	for rows.Next() {
		err = rows.Scan(&name, &surname)
		if err != nil {
			t.Fatalf("Cannot scan row: %s\n", err)
		}
		if surname != "Doe" {
			t.Fatalf("Didn't expect surname beeing %s", surname)
		}
		names = append(names, name)
	}

	if len(names) != 3 {
		t.Fatalf("Expected 3 rows, not %d", len(names))
	}

	if names[0] != "Jane" {
		t.Fatalf("Wanted Jane, got %s", names[0])
	}

	if names[1] != "Joe" {
		t.Fatalf("Wanted Joe, got %s", names[1])
	}

	if names[2] != "John" {
		t.Fatalf("Wanted John, got %s", names[2])
	}

}

func TestOrderByLimit(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestOrderByLimit")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

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

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE 1=1 ORDER BY age DESC LIMIT 2`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot limit select: %s", err)
	}

	var age, last, size int64
	last = 4000
	// Now every time 'age' should be less than 'last'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if age > last {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 2 {
		t.Fatalf("Expecting 2 rows here, got %d", size)
	}
}

func TestOrderByIntEmpty(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestOrderByIntEmpty")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE surname = Wayne OR surname = Doe ORDER BY age DESC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}
	defer rows.Close()

}
