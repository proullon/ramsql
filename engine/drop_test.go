package engine_test

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func TestDrop(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestDrop")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	log.SetLevel(log.DebugLevel)
	_, err = db.Exec("DROP TABLE account")
	if err != nil {
		t.Fatalf("cannot drop table: %s", err)
	}
}
