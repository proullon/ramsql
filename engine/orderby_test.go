package engine_test

import (
	"database/sql"
	"fmt"
	"github.com/proullon/ramsql/engine/log"
	"testing"
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
			t.Fatalf("Didn't expect surname being %s", surname)
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

func TestOrderByMultipleStrings(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestOrderByMultipleStrings")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT64, savings DECIMAL);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Angel, 11, 125.215);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Angel, 11, 1.1);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Zebra, 32, 0.921);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Anna, Angel, 33, 0);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Anna, Zebra, 9, 25);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT name, surname, age, savings FROM user ORDER BY name ASC, age DESC, savings ASC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}

	exp := [][]string{
		{"Anna", "33", "0"},
		{"Anna", "9", "25"},
		{"Joe", "32", "0.921"},
		{"Joe", "11", "1.1"},
		{"Joe", "11", "125.215"},
	}
	var (
		got           [][]string
		name, surname string
		age           int64
		savings       float64
	)
	for rows.Next() {
		err = rows.Scan(&name, &surname, &age, &savings)
		if err != nil {
			t.Fatalf("Cannot scan row: %s\n", err)
		}
		got = append(got, []string{name, fmt.Sprint(age), fmt.Sprint(savings)})
	}

	if len(exp) != len(got) {
		t.Fatalf("length mismatch, expected %d but got %d", len(exp), len(got))
	}
	for i := 0; i < len(exp); i++ {
		for j := 0; j < len(exp[i]); j++ {
			if exp[i][j] != got[i][j] {
				t.Fatalf("data mismatch at %d/%d, expected %v but got %v", i, j, exp[i][j], got[i][j])
			}
		}
	}
}
