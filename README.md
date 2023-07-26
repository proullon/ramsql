# RamSQL

[![Build Status](https://travis-ci.org/proullon/ramsql.svg)](https://travis-ci.org/proullon/ramsql)

## Disposable SQL engine

RamSQL has been written to be used in your project's test suite.

Unit testing in Go is simple, create a foo_test.go import testing and run `go test ./...`.
But then there is SQL queries, constraints, CRUD...and suddenly you need a PostgresSQL, setup scripts and nothing is easy anymore.

The idea is to avoid setup, DBMS installation and credentials management as long as possible.
A unique engine is tied to a single sql.DB with as much sql.Conn as needed providing a unique DataSourceName.
Bottom line : One DataSourceName per test and you have full test isolation in no time.

## Installation

```
  go get github.com/proullon/ramsql
```

## Usage

Let's say you want to test the function LoadUserAddresses :

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

Use RamSQL to test it in a disposable isolated in-memory SQL engine :

```go
package myproject 

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/proullon/ramsql/driver"
)


func TestLoadUserAddresses(t *testing.T) {
	batch := []string{
		`CREATE TABLE address (id BIGSERIAL PRIMARY KEY, street TEXT, street_number INT);`,
		`CREATE TABLE user_addresses (address_id INT, user_id INT);`,
		`INSERT INTO address (street, street_number) VALUES ('rue Victor Hugo', 32);`,
		`INSERT INTO address (street, street_number) VALUES ('boulevard de la République', 23);`,
		`INSERT INTO address (street, street_number) VALUES ('rue Charles Martel', 5);`,
		`INSERT INTO address (street, street_number) VALUES ('chemin du bout du monde ', 323);`,
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
		t.Fatalf("sql.Open : Error : %s\n", err)
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
		t.Fatalf("Too bad! unexpected error: %s", err)
	}

	if len(addresses) != 2 {
		t.Fatalf("Expected 2 addresses, got %d", len(addresses))
	}

}
```

Done. No need for a running PostgreSQL or a setup. Your tests are isolated, and compliant with go tools.

## RamSQL binary

Let's say you have a SQL describing your application structure:

```sql
CREATE TABLE IF NOT EXISTS address (id BIGSERIAL PRIMARY KEY, street TEXT, street_number INT);
CREATE TABLE IF NOT EXISTS user_addresses (address_id INT, user_id INT);
```

You may want to test its validity:

```console
$ go install github.com/proullon/ramsql
$ ramsql < schema.sql
ramsql> Query OK. 1 rows affected
ramsql> Query OK. 1 rows affected
$ echo $?
0
```

## Features

Find bellow all objectives for `v1.0.0`

| Name           | Category      | Parsing                  | Implementation           |
| -------------- | ------------- | ------------------------ | ------------------------ |
| Table          | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| Schema         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| CREATE         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| PRIMARY_KEY    | SQL           | :heavy_check_mark:       | :heavy_multiplication_x: |
| DEFAULT        | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| INSERT         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| UNIQUE         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| FOREIGN KEY    | SQL           | :heavy_multiplication_x: | :heavy_multiplication_x: |
| SELECT         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| backtick       | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| quote          | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| double quote   | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| COUNT          | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| MAX            | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| ORDER BY       | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| UPDATE         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| DELETE         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| DROP           | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| INNER JOIN     | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| OUTER JOIN     | SQL           | :heavy_check_mark:       | :heavy_multiplication_x: |
| timestamp      | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| now()          | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| OFFSET         | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| Transactions   | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| BEGIN          | SQL           | :heavy_multiplication_x: | :heavy_multiplication_x: |
| COMMIT         | SQL           | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Index          | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| Hash index     | SQL           | :heavy_check_mark:       | :heavy_check_mark:       |
| B-Tree index   | SQL           | :heavy_check_mark:       | :heavy_multiplication_x: |
| JSON           | SQL           | :heavy_multiplication_x: | :heavy_multiplication_x: |
| AS             | SQL           | :heavy_multiplication_x: | :heavy_multiplication_x: |
| CLI            | Testing       | :heavy_check_mark:       | :heavy_check_mark:       |
| Breakpoint     | Testing       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Query history  | Testing       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Size limit     | Testing       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Autogeneration | Testing       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| TTL            | Caching       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| LFRU           | Caching       | :heavy_multiplication_x: | :heavy_multiplication_x: |
| Gorm           | Compatibility | :heavy_check_mark:       | :heavy_check_mark:       |

### Unit testing

- Full isolation between tests
- No setup (either file or databases)
- Good performance

### SQL parsing

- Database schema validation
- ALTER file validation

### Stress testing

- File system full error with configurable maximum database size
- Random configurable slow queries
- Random connection error

## Compatibility

### GORM

If you intend to use ramsql with the GORM ORM, you should use the GORM Postgres driver. A working example would be:

```go
import (
	"database/sql"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Product struct {
	gorm.Model
	Code       string
	Price      uint
	TestBigint uint64 `gorm:"test_bigint;type:BIGINT UNSIGNED AUTO_INCREMENT"`
}

// From https://gorm.io/docs/connecting_to_the_database.html
// and  https://gorm.io/docs/
func main() {
	ramdb, err := sql.Open("ramsql", "TestGormQuickStart")

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: ramdb,
	}),
		&gorm.Config{})

	// Migrate the schema
	err = db.AutoMigrate(&Product{})

	// Create
	err = db.Create(&Product{Code: "D42", Price: 100}).Error

	// Read
	var product Product
	err = db.First(&product, 1).Error // find product with integer primary key
	err = db.First(&product, "code = ?", "D42").Error // find product with code D42
	err = db.First(&product, "Code = ?", "D42").Error // find product with code D42

	// Update - update product's price to 200
	err = db.Model(&product).Update("Price", 200).Error
	// Update - update multiple fields
	err = db.Model(&product).Updates(Product{Price: 200, Code: "F42"}).Error // non-zero fields
	err = db.Model(&product).Updates(map[string]interface{}{"Price": 200, "Code": "F42"}).Error

	// Delete - delete product
	err = db.Delete(&product, 1).Error

    _ = err
}
```

## Architecture

### Rows storage and garbage collector

What options do we have to store objects:

- unsafe memory paging
- map
- slice
- linked list

The issue with having a lot of objects (rows here) in memory is Garbage Collector pause. The processus will lock objects to determine if there is any pointers to it. This becomes an issue with `map[any]*Something` since GC will lock the map to check all pointers.

Unsafe memory paging is a bit tricky to keep portable.

Slices are nice, and could grow non linearly capped with available RAM.

The simplest option regarding GC pause and rows storage is linked list. Easy to update and remove rows without overhead, while keeping GC functioning properly.

### Indexes

We want Hash index to fetch rows in `O(1)` time with `=` operator. This means we need to use a map, without using pointers. That's where `uintptr` comes to play. Hash index uses `map[string]uintptr` or `map[int64]uintptr` to keep track of pointer to linked list elements, while discarding GC checks.

We also want Binary Tree index to fetch rows in `O(log(n))` time with `<, <=, >, >=` operators.

### Transactions

`RamSQL` only uses table level lock transactions. In case of error or call to `Rollback()`, changes will be reverted back into modified relation.

`Commit()` releases the locks.

## TODO

- `agnostic` -> `memstore`
- `executor` -> `sql`
