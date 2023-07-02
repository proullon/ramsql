package main_test

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	_ "github.com/proullon/ramsql/driver"
)

func setupInsertN(b *testing.B, db *sql.DB, n int) {
	db.Exec(`DROP TABLE account`)
	_, err := db.Exec(`CREATE TABLE account (id BIGSERIAL PRIMARY KEY, email TEXT)`)
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

}

func benchmarkInsert(b *testing.B, db *sql.DB, nbRows int) {

	db.Exec(`DROP TABLE account`)
	_, err := db.Exec(`CREATE TABLE account (id BIGSERIAL PRIMARY KEY, email TEXT)`)
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

func benchmarkSelect(b *testing.B, db *sql.DB) {

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

	_, err := db.Exec(`DROP TABLE account`)
	if err != nil {
		b.Fatalf("sql.Exec: %s", err)
	}
}

func BenchmarkRamSQLSelect(b *testing.B) {
	db, err := sql.Open("ramsql", "BenchmarkSQLSelect")
	if err != nil {
		b.Fatalf("cannot open ramsql db")
	}

	n := 100
	setupInsertN(b, db, n)
	benchmarkSelect(b, db)
}

func BenchmarkSQLiteSelect(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("cannot open sqlite")
	}

	n := 100
	setupInsertN(b, db, n)
	benchmarkSelect(b, db)
}

func BenchmarkRamSQLSelect10K(b *testing.B) {
	db, err := sql.Open("ramsql", "BenchmarkSQLSelect1M")
	if err != nil {
		b.Fatalf("cannot open ramsql db")
	}

	n := 10000
	setupInsertN(b, db, n)
	benchmarkSelect(b, db)
}

func BenchmarkSQLiteSelect10K(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("cannot open sqlite")
	}

	n := 10000
	setupInsertN(b, db, n)
	benchmarkSelect(b, db)
}

func BenchmarkRamSQLInsert10(b *testing.B) {
	db, err := sql.Open("ramsql", "BenchmarkSQLSelect")
	if err != nil {
		b.Fatalf("cannot open ramsql db")
	}
	benchmarkInsert(b, db, 10)
}

func BenchmarkSQLiteInsert10(b *testing.B) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("cannot open sqlite")
	}
	benchmarkInsert(b, db, 10)
}

func BenchmarkRamSQLSetup(b *testing.B) {
	for n := 0; n < b.N; n++ {
		db, err := sql.Open("ramsql", "BenchmarkSQLSelect")
		if err != nil {
			b.Fatalf("cannot open ramsql db")
		}
		db.Close()
	}
}

func BenchmarkSQLiteSetup(b *testing.B) {
	for n := 0; n < b.N; n++ {
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			b.Fatalf("cannot open sqlite")
		}
		db.Close()
	}
}
