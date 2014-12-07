package ramsql

import (
	"database/sql"
	"fmt"
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

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (1, 'foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('id', 'email') VALUES (2, 'bar@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rows, err := db.Query("SELECT * FROM account WHERE email = '$1'", "foo@bar.com")
	if err != nil {
		t.Fatalf("sql.Query error : %s\n", err)
	}

	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("ERROR : %s\n", err)
		return
	}

	// print rows name
	prettyPrintHeader(columns)

	for rows.Next() {
		holders := make([]interface{}, len(columns))
		for i := range holders {
			holders[i] = new(string)
		}
		err := rows.Scan(holders...)
		if err != nil {
			t.Fatalf("ERROR : %s\n", err)
		}
		prettyPrintRow(holders)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}

}

func prettyPrintHeader(row []string) {
	for i, r := range row {
		if i != 0 {
			fmt.Printf("  |  ")
		}
		fmt.Printf("%-6s", r)
	}
	fmt.Printf("\n\n")
}

func prettyPrintRow(row []interface{}) {
	for i, r := range row {
		if i != 0 {
			fmt.Printf("  |  ")
		}
		s, ok := r.(*string)
		if !ok {
			panic("lo")
		}
		fmt.Printf("%-6s", *s)
	}
	fmt.Println()
}
