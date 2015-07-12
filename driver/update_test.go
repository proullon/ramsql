package ramsql

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"
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
