package main_test

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/proullon/ramsql/driver"
)

func benchmarkInsert(b *testing.B, driver string, nbRows int) {
	u := os.Getenv("PG_USER")
	pwd := os.Getenv("PG_PASSWORD")
	ip := os.Getenv("PG_IP")
	port := os.Getenv("PG_PORT")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=disable", u, pwd, ip, port)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		b.Fatalf("sql.Open: %s", err)
	}

	db.Exec(`DROP TABLE account`)
	_, err = db.Exec(`CREATE TABLE account (id BIGSERIAL PRIMARY KEY, email TEXT)`)
	if err != nil {
		b.Fatalf("sql.Exec: %s", err)
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		query := `INSERT INTO account (email) VALUES ($1)`
		for i := 0; i < nbRows; i++ {
			size := 32
			bs := make([]byte, size)
			_, err := rand.Read(bs)
			if err != nil {
				b.Fatalf("rand failed: %s", err)
			}
			str := hex.EncodeToString(bs)
			token := []byte(str)[0:size]

			_, err = db.Exec(query, string(token)+"@foobar.com")
			if err != nil {
				b.Fatalf("cannot insert rows: %s", err)
			}
		}
	}

	_, err = db.Exec(`DROP TABLE account`)
	if err != nil {
		b.Fatalf("sql.Exec: %s", err)
	}
}

func benchmarkSelect(b *testing.B, driver string, n int) {
	u := os.Getenv("PG_USER")
	pwd := os.Getenv("PG_PASSWORD")
	ip := os.Getenv("PG_IP")
	port := os.Getenv("PG_PORT")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=disable", u, pwd, ip, port)
	db, err := sql.Open(driver, dsn)
	if err != nil {
		b.Fatalf("sql.Open: %s", err)
	}

	db.Exec(`DROP TABLE account`)
	_, err = db.Exec(`CREATE TABLE account (id BIGSERIAL PRIMARY KEY, email TEXT)`)
	if err != nil {
		b.Fatalf("sql.Exec: %s", err)
	}

	query := `INSERT INTO account (email) VALUES ($1)`
	for i := 0; i < n; i++ {
		size := 32
		bs := make([]byte, size)
		_, err := rand.Read(bs)
		if err != nil {
			b.Fatalf("rand failed: %s", err)
		}
		str := hex.EncodeToString(bs)
		token := []byte(str)[0:size]

		_, err = db.Exec(query, string(token)+"@foobar.com")
		if err != nil {
			b.Fatalf("cannot insert rows: %s", err)
		}
	}

	var id int64
	var email string

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		query := `SELECT account.id, account.email FROM account WHERE id > $1 AND id < $2`
		rows, err := db.Query(query, 20, 50)
		if err != nil {
			b.Fatalf("cannot query rows: %s", err)
		}

		for rows.Next() {
			err = rows.Scan(&id, &email)
			if err != nil {
				b.Fatalf("cannot scan rows: %s", err)
			}
		}
	}

	_ = id
	_ = email

	_, err = db.Exec(`DROP TABLE account`)
	if err != nil {
		b.Fatalf("sql.Exec: %s", err)
	}
}

func BenchmarkRamSQLSelect(b *testing.B) {
	benchmarkSelect(b, "ramsql", 100)
}

func BenchmarkPostgresSelect(b *testing.B) {
	benchmarkSelect(b, "postgres", 100)
}

func BenchmarkRamSQLInsert10(b *testing.B) {
	benchmarkInsert(b, "ramsql", 10)
}

func BenchmarkPostgresInsert10(b *testing.B) {
	benchmarkInsert(b, "postgres", 10)
}
