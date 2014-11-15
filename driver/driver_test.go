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

// func TestSelect(t *testing.T) {
// 	db, err := sql.Open("ramsql", "")
// 	if err != nil {
// 		t.Fatalf("sql.Open : Error : %s\n", err)
// 	}
// 	defer db.Close()

// 	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
// 	if err != nil {
// 		t.Fatalf("sql.Exec: Error: %s\n", err)
// 	}

// 	rows, err := db.Query("SELECT * FROM account WHERE email = '?'", "foo@bar.com")
// 	if err != nil {
// 		t.Fatalf("sql.Query error : %s\n", err)
// 	}

// 	i := 0
// 	for rows.Next() {
// 		i++
// 	}
// 	t.Logf("%d rows affected\n", i)

// 	err = db.Close()
// 	if err != nil {
// 		t.Fatalf("sql.Close : Error : %s\n", err)
// 	}
// }
