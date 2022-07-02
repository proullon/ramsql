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

### Unit testing

- Full isolation between tests
- No setup (either file or databases)
- Good performance

### SQL parsing

- Databse schema validation
- ALTER file validation

### Stress testing

- File system full error with configurable maximum database size
- Random configurable slow queries
- Random deconnection

## Compatibility

### GORM
If you intend to use ramsql with the GORM ORM, you should use the GORM Postgres driver. A working example would be:
```go
	sqlDB, err := sql.Open("ramsql", "Test")
	...

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: sqlDB,
	}), &gorm.Config{})
```
