package ramsql

import (
	"database/sql"
	"testing"
)

func TestCreateTable(t *testing.T) {
	db, err := sql.Open("ramsql", "")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}
}

func TestInsertTable(t *testing.T) {
	db, err := sql.Open("ramsql", "")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	res, err := db.Exec("INSERT INTO account ('id', 'email') VALUES (1, 'foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	res, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (2, 'roger@gmail.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	aff, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot get the number of rows affected: %s", err)
	}

	t.Logf("%d rows affected\n", aff)

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}
}

func TestSelect(t *testing.T) {
	db, err := sql.Open("ramsql", "")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (2, 'bar@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (1, 'foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rows, err := db.Query("SELECT * FROM account WHERE email = '$1'", "foo@bar.com")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("rows.Column : %s", err)
		return
	}

	if len(columns) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(columns))
	}

	row := db.QueryRow("SELECT * FROM account WHERE email = '$1'", "foo@bar.com")
	if row == nil {
		t.Fatalf("sql.QueryRow error")
	}

	var email string
	var id int
	err = row.Scan(&id, &email)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id = 1, got %d", id)
	}

	if email != "foo@bar.com" {
		t.Fatalf("Expected email = <foo@bar.com>, got <%s>", email)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}

}
