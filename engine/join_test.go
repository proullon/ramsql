package engine_test

import (
	"database/sql"
	"testing"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func TestJoinOrderBy(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestJoinOrderBy")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE user (id BIGSERIAL, name TEXT)`,
		`CREATE TABLE address (id BIGSERIAL, user_id INT, value TEXT)`,
		`INSERT INTO user (name) VALUES ($$riri$$)`,
		`INSERT INTO user (name) VALUES ($$fifi$$)`,
		`INSERT INTO user (name) VALUES ($$loulou$$)`,
		`INSERT INTO address (user_id, value) VALUES (1, 'rue du puit')`,
		`INSERT INTO address (user_id, value) VALUES (1, 'rue du désert')`,
		`INSERT INTO address (user_id, value) VALUES (3, 'rue du chemin')`,
		`INSERT INTO address (user_id, value) VALUES (2, 'boulevard du con')`,
	}
	for _, q := range init {
		_, err := db.Exec(q)
		if err != nil {
			t.Fatalf("Cannot initialize test: %s", err)
		}
	}

	query := `SELECT user.name, address.value 
			FROM user 
			JOIN address ON address.user_id = user.id
			WHERE user.id = $1
			ORDER BY address.value ASC`
	rows, err := db.Query(query, 1)
	if err != nil {
		t.Fatalf("Cannot select with joined order by: %s", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("Expected 2 rows, got %d", n)
	}
}
