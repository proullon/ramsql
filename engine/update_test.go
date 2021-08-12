package engine_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func TestUpdateSimple(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestUpdateSimple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("UPDATE account SET email = 'roger@gmail.com' WHERE id = 2")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	row := db.QueryRow("SELECT * FROM account WHERE id = 2")
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var email string
	var id int
	err = row.Scan(&id, &email)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if email != "roger@gmail.com" {
		t.Fatalf("Expected email 'roger@gmail.com', got '%s'", email)
	}
}

func TestUpdateIsNull(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestUpdateIsNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	res, err := db.Exec("UPDATE account SET email = 'roger@gmail.com', creation_date = $1 WHERE id = 2 AND creation_date IS NULL", time.Now())
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	ra, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check number of rows affected: %s", err)
	}
	if ra != 1 {
		t.Fatalf("Expected 1 row, affected. Got %d", ra)
	}

	rows, err := db.Query(`SELECT id FROM account WHERE creation_date IS NULL`)
	if err != nil {
		t.Fatalf("cannot select null columns: %s", err)
	}

	var n, id int64
	for rows.Next() {
		n++
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("cannot scan null columns: %s", err)
		}
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("Expected 1 rows, got %d", n)
	}

	rows, err = db.Query(`SELECT id FROM account WHERE creation_date IS NOT NULL`)
	if err != nil {
		t.Fatalf("cannot select not null columns: %s", err)
	}

	n = 0
	for rows.Next() {
		n++
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("cannot scan null columns: %s", err)
		}
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("Expected 1 rows, got %d", n)
	}

}

func TestUpdateNotNull(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestUpdateNotNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("UPDATE account SET email = 'roger@gmail.com' WHERE id = 2 AND creation_date IS NOT NULL")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

}

func TestUpdateToNull(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestUpdateToNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	row1 := db.QueryRow("SELECT email FROM account WHERE id = 1")
	if row1 == nil {
		t.Fatalf("sql.Query failed")
	}

	var email1 *string
	err = row1.Scan(&email1)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if email1 == nil {
		t.Fatalf("expected 'foo@bar.com' email, but got NULL")
	}

	_, err = db.Exec("UPDATE account SET email = NULL WHERE id = 1")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	row2 := db.QueryRow("SELECT email FROM account WHERE id = 1")
	if row2 == nil {
		t.Fatalf("sql.Query failed")
	}

	var email2 *string
	err = row2.Scan(&email2)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if email2 != nil {
		t.Fatalf("expected NULL email, but got '%v'", *email2)
	}

}
