package engine_test

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func TestTrunc(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestTrunc")
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

	rows, err := db.Query("SELECT * FROM account WHERE 1")
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

	res, err := db.Exec("DELETE FROM account")
	if err != nil {
		t.Fatalf("Cannot truncate table: %s", err)
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot fetch affected rows: %s", err)
	}

	if affectedRows != 2 {
		t.Fatalf("Expected 2 rows affected, got %d", affectedRows)
	}

}

func TestDelete(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestDelete")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE queue (id BIGSERIAL, data TEXT)`,
		`INSERT INTO queue (data) VALUES ($$foo$$)`,
		`INSERT INTO queue (data) VALUES ($$bar$$)`,
		`INSERT INTO queue (data) VALUES ($$foobar$$)`,
		`INSERT INTO queue (data) VALUES ($$barfoo$$)`,
		`INSERT INTO queue (data) VALUES ($$ok$$)`,
	}
	for _, q := range init {
		_, err := db.Exec(q)
		if err != nil {
			t.Fatalf("Cannot initialize test: %s", err)
		}
	}

	query := `DELETE FROM queue WHERE id = $1`

	// Delete last
	_, err = db.Exec(query, 5)
	if err != nil {
		t.Fatalf("cannot delete last: %s", err)
	}

	// Delete first
	_, err = db.Exec(query, 1)
	if err != nil {
		t.Fatalf("cannot delete first: %s", err)
	}

	// Delete middle
	_, err = db.Exec(query, 3)
	if err != nil {
		t.Fatalf("cannot delete in the midde: %s", err)
	}

	_, err = db.Exec(query, 4)
	if err != nil {
		t.Fatalf("cannot delete row: %s", err)
	}

	// Delete last
	_, err = db.Exec(query, 2)
	if err != nil {
		t.Fatalf("cannot delete last row in table: %s", err)
	}
}

func TestDeleteAnd(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestDeleteAnd")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE foo (id BIGSERIAL, bar_id INT, toto_id INT)`,
		`INSERT INTO foo (bar_id, toto_id) VALUES (2, 3)`,
		`INSERT INTO foo (bar_id, toto_id) VALUES (4, 32)`,
		`INSERT INTO foo (bar_id, toto_id) VALUES (5, 33)`,
		`INSERT INTO foo (bar_id, toto_id) VALUES (6, 4)`,
	}
	for _, q := range init {
		_, err := db.Exec(q)
		if err != nil {
			t.Fatalf("Cannot initialize test: %s", err)
		}
	}

	query := `DELETE FROM foo WHERE bar_id = $1 AND toto_id = $2`
	_, err = db.Exec(query, 4, 32)
	if err != nil {
		t.Fatalf("cannot delete: %s", err)
	}

	n := 0
	query = `SELECT bar_id FROM foo WHERE 1=1`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("cannot query foo: %s", err)
	}
	for rows.Next() {
		n++
	}
	if n != 3 {
		t.Fatalf("Expected 3 values, got %d", n)
	}
}
