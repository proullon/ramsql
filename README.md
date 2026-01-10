# RamSQL

[![Go](https://github.com/leonardaustin/ramsql/actions/workflows/go.yml/badge.svg)](https://github.com/leonardaustin/ramsql/actions/workflows/go.yml)

> **Note:** This is a fork of [proullon/ramsql](https://github.com/proullon/ramsql). All credit for the original implementation goes to the creators and contributors of that project. We aim to contribute our improvements back to the upstream repository when possible.

## Disposable SQL Engine for Testing

RamSQL is a lightweight, in-memory SQL engine written in Go, specifically designed for unit testing. It eliminates the need for a running PostgreSQL or MySQL instance during tests, providing full isolation and zero setup overhead.

### Why RamSQL?

Unit testing in Go is simple: create a `foo_test.go`, import `testing`, and run `go test ./...`. But when SQL queries enter the picture, you suddenly need database setup scripts, credentials management, and infrastructure complexity.

RamSQL solves this by providing:
- **Full test isolation** - One DataSourceName per test means completely independent test runs
- **Zero setup** - No database installation or configuration required
- **Standard interface** - Implements Go's `database/sql/driver` interface
- **Fast execution** - Pure in-memory operations

## Installation

```bash
go get github.com/leonardaustin/ramsql
```

## Quick Start

Here's how to test a function that queries user addresses:

```go
func LoadUserAddresses(db *sql.DB, userID int64) ([]string, error) {
	query := `SELECT address.street_number, address.street FROM address
			  JOIN user_addresses ON address.id=user_addresses.address_id
			  WHERE user_addresses.user_id = $1;`

	rows, err := db.Query(query, userID)
	if err != nil {
		return nil, err
	}

	var addresses []string
	for rows.Next() {
		var number int
		var street string
		if err := rows.Scan(&number, &street); err != nil {
			return nil, err
		}
		addresses = append(addresses, fmt.Sprintf("%d %s", number, street))
	}

	return addresses, nil
}
```

Test it with RamSQL:

```go
package myproject

import (
	"database/sql"
	"testing"

	_ "github.com/leonardaustin/ramsql/driver"
)

func TestLoadUserAddresses(t *testing.T) {
	batch := []string{
		`CREATE TABLE address (id BIGSERIAL PRIMARY KEY, street TEXT, street_number INT);`,
		`CREATE TABLE user_addresses (address_id INT, user_id INT);`,
		`INSERT INTO address (street, street_number) VALUES ('rue Victor Hugo', 32);`,
		`INSERT INTO address (street, street_number) VALUES ('boulevard de la République', 23);`,
		`INSERT INTO address (street, street_number) VALUES ('rue Charles Martel', 5);`,
		`INSERT INTO address (street, street_number) VALUES ('chemin du bout du monde', 323);`,
		`INSERT INTO address (street, street_number) VALUES ('boulevard de la liberté', 2);`,
		`INSERT INTO address (street, street_number) VALUES ('avenue des champs', 12);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 1);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 1);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 2);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 3);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 4);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 5);`,
	}

	db, err := sql.Open("ramsql", "TestLoadUserAddresses")
	if err != nil {
		t.Fatalf("sql.Open: Error: %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	addresses, err := LoadUserAddresses(db, 1)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if len(addresses) != 2 {
		t.Fatalf("Expected 2 addresses, got %d", len(addresses))
	}
}
```

No running PostgreSQL. No setup scripts. Full test isolation compliant with Go tools.

## CLI Tool

RamSQL includes a command-line interface for validating SQL schemas:

```bash
go install github.com/leonardaustin/ramsql@latest
```

Test your schema files:

```console
$ ramsql < schema.sql
ramsql> Query OK. 1 rows affected
ramsql> Query OK. 1 rows affected
$ echo $?
0
```

## Features

| Feature | Parsing | Implementation | Notes |
|---------|---------|----------------|-------|
| **DDL** |
| CREATE TABLE | :heavy_check_mark: | :heavy_check_mark: | IF NOT EXISTS supported |
| CREATE INDEX | :heavy_check_mark: | :heavy_check_mark: | |
| CREATE SCHEMA | :heavy_check_mark: | :heavy_check_mark: | |
| DROP TABLE | :heavy_check_mark: | :heavy_check_mark: | |
| TRUNCATE | :heavy_check_mark: | :heavy_check_mark: | |
| **DML** |
| SELECT | :heavy_check_mark: | :heavy_check_mark: | |
| INSERT | :heavy_check_mark: | :heavy_check_mark: | Single and multi-row |
| UPDATE | :heavy_check_mark: | :heavy_check_mark: | |
| DELETE | :heavy_check_mark: | :heavy_check_mark: | |
| **Constraints** |
| PRIMARY KEY | :heavy_check_mark: | :heavy_check_mark: | |
| UNIQUE | :heavy_check_mark: | :heavy_check_mark: | |
| DEFAULT | :heavy_check_mark: | :heavy_check_mark: | |
| FOREIGN KEY | :heavy_multiplication_x: | :heavy_multiplication_x: | Not implemented |
| **Joins** |
| INNER JOIN | :heavy_check_mark: | :heavy_check_mark: | |
| OUTER JOIN | :heavy_check_mark: | :heavy_multiplication_x: | Parsed only |
| **Clauses** |
| WHERE | :heavy_check_mark: | :heavy_check_mark: | AND, OR, brackets |
| ORDER BY | :heavy_check_mark: | :heavy_check_mark: | ASC, DESC |
| LIMIT | :heavy_check_mark: | :heavy_check_mark: | |
| OFFSET | :heavy_check_mark: | :heavy_check_mark: | |
| DISTINCT | :heavy_check_mark: | :heavy_check_mark: | |
| GROUP BY | :heavy_check_mark: | :heavy_check_mark: | |
| **Operators** |
| Comparison (=, !=, <, >, <=, >=) | :heavy_check_mark: | :heavy_check_mark: | |
| IN / NOT IN | :heavy_check_mark: | :heavy_check_mark: | |
| LIKE | :heavy_check_mark: | :heavy_check_mark: | % and _ wildcards |
| ILIKE | :heavy_check_mark: | :heavy_check_mark: | Case-insensitive LIKE |
| IS NULL / IS NOT NULL | :heavy_check_mark: | :heavy_check_mark: | |
| **Aggregates** |
| COUNT | :heavy_check_mark: | :heavy_check_mark: | With filter support |
| MAX | :heavy_check_mark: | :heavy_check_mark: | |
| **Transactions** |
| BEGIN/COMMIT/ROLLBACK | :heavy_check_mark: | :heavy_check_mark: | Table-level locking |
| **Indexes** |
| Hash Index | :heavy_check_mark: | :heavy_check_mark: | O(1) lookups |
| B-Tree Index | :heavy_check_mark: | :heavy_multiplication_x: | Parsed only |
| **Types** |
| INT/BIGINT | :heavy_check_mark: | :heavy_check_mark: | |
| TEXT/VARCHAR | :heavy_check_mark: | :heavy_check_mark: | |
| BOOLEAN | :heavy_check_mark: | :heavy_check_mark: | |
| TIMESTAMP/DATE | :heavy_check_mark: | :heavy_check_mark: | |
| BIGSERIAL | :heavy_check_mark: | :heavy_check_mark: | Auto-increment |
| FLOAT/REAL | :heavy_check_mark: | :heavy_check_mark: | |
| BYTEA | :heavy_check_mark: | :heavy_check_mark: | |
| JSON | :heavy_multiplication_x: | :heavy_multiplication_x: | Not implemented |
| **Other** |
| Backticks/Quotes | :heavy_check_mark: | :heavy_check_mark: | Multiple quoting styles |
| now() | :heavy_check_mark: | :heavy_check_mark: | |
| RETURNING | :heavy_check_mark: | :heavy_check_mark: | For INSERT |
| Type casting | :heavy_check_mark: | :heavy_check_mark: | |
| AS (aliases) | :heavy_multiplication_x: | :heavy_multiplication_x: | Not implemented |

## GORM Compatibility

RamSQL works with GORM using the PostgreSQL driver:

```go
import (
	"database/sql"
	"testing"

	_ "github.com/leonardaustin/ramsql/driver"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code  string
	Price uint
}

func TestWithGORM(t *testing.T) {
	ramdb, err := sql.Open("ramsql", "TestGORM")
	if err != nil {
		t.Fatal(err)
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: ramdb,
	}), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}

	// AutoMigrate, Create, Read, Update, Delete all work
	db.AutoMigrate(&Product{})
	db.Create(&Product{Code: "D42", Price: 100})

	var product Product
	db.First(&product, "code = ?", "D42")
}
```

## Architecture

### Storage Design

RamSQL uses a linked list (`container/list`) to store rows, which minimizes garbage collector pause times. Using `map[any]*Something` would cause GC to lock and check all pointers, but linked lists avoid this overhead.

### Indexing

- **Hash indexes** use `map[string]uintptr` or `map[int64]uintptr` for O(1) lookups with the `=` operator
- **B-Tree indexes** are planned for O(log n) range queries with `<, <=, >, >=` operators

### Transactions

RamSQL uses table-level locking for transactions. Changes are tracked and can be reverted on `Rollback()`. `Commit()` releases locks and clears the change history.

## Development

```bash
# Run tests
make test

# Run tests with coverage report
make report

# Run benchmarks
make bench

# Build and install
make install
```

## Known Limitations

- OUTER JOIN is parsed but not implemented
- B-Tree index optimization is parsed but not fully implemented
- FOREIGN KEY constraints are not supported
- JSON type is not supported
- Query aliases (AS) are not supported

## License

MIT
