package ramsql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/proullon/ramsql/engine/log"
)

func TestCreateTable(t *testing.T) {

	db, err := sql.Open("ramsql", "TestCreateTable")
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

func TestInsertBool(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertBool")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}

	_, err = db.Exec(`CREATE TABLE ttable (id BIGINT, hasBool BOOL)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	_, err = db.Exec(`INSERT INTO ttable (id, hasBool) VALUES (?,?)`, 1, true)
	if err != nil {
		t.Fatalf("cannot insert into table: %s", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}

}

func TestInsertEmptyString(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertEmptyString")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account (id, email) VALUES (1, '')")
	if err != nil {
		t.Fatalf("Cannot insert empty string: %s", err)
	}

}

func TestCreateTableIfNotExists(t *testing.T) {

	db, err := sql.Open("ramsql", "TestCreateTableIfNotExists")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: cannot create table: %s\n", err)
	}

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err == nil {
		t.Fatalf("sql.Exec: table already exists, expected error")
	}

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: IF NOT EXISTS is ignored: %s\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}
}

func TestInsertTable(t *testing.T) {
	db, err := sql.Open("ramsql", "TestInsertTable")
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

	res, err := db.Exec("INSERT INTO account ('id', 'email') VALUES (2, 'roger@gmail.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	aff, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot get the number of rows affected: %s", err)
	}

	_ = aff
	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}
}

func TestSelectWhereAttribute(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSelectWhereAttribute")
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

	rows, err := db.Query(`SELECT * FROM account WHERE "account".id = 1`)
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
}

func TestSelectCamelCase(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSelectCamelCase")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email_snake TEXT, TestCamelCase TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Query(`SELECT email_snake FROM account WHERE "account".id = 1`)
	if err != nil {
		t.Fatalf("sql.Query snake_case error : %s", err)
	}

	_, err = db.Query(`SELECT TestCamelCase FROM account WHERE "account".id = 1`)
	if err != nil {
		t.Fatalf("sql.Query CamelCase error : %s", err)
	}

	_, err = db.Query(`SELECT TestCamelCase, nope, email_snake FROM account WHERE 1`)
	if err == nil {
		t.Fatalf("expected attribute not found error")
	}
	ee := "attribute not defined: account.nope"
	if err.Error() != ee {
		t.Fatalf("expected error to be '%s', got '%s'", ee, err.Error())
	}
}

func TestSelectSimplePredicate(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSelectSimplePredicate")
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
}

func TestMultipleCreate(t *testing.T) {
	db, err := sql.Open("ramsql", "TestMultipleCreate")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err == nil {
		t.Fatalf("Should not have been able to recreate table account")
	}
}

func TestCreateTableWithTimestamp(t *testing.T) {

	query := `create table if not exists "refresh_token" ("uuid" text not null primary key,
	"hash_token" text,
	"user_id" bigint,
	"expires" timestamp with time zone,
	"tag" text) ;`

	db, err := sql.Open("ramsql", "TestCreateTableWithTimestamp")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}
}

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

func TestBatch(t *testing.T) {

	batch := []string{
		`CREATE TABLE address (id BIGSERIAL PRIMARY KEY, street TEXT, street_number INT);`,
		`CREATE TABLE user_addresses (address_id BIGINT, user_id BIGINT);`,
		`INSERT INTO address (street, street_number) VALUES ('rue victor hugo', 32);`,
		`INSERT INTO address (street, street_number) VALUES ('boulevard de la république', 23);`,
		`INSERT INTO address (street, street_number) VALUES ('rue charles martel', 5);`,
		`INSERT INTO address (street, street_number) VALUES ('rue victoire', 323);`,
		`INSERT INTO address (street, street_number) VALUES ('boulevard de la liberté', 2);`,
		`INSERT INTO address (street, street_number) VALUES ('avenue des champs', 12);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 1);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 1);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 2);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (2, 3);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 4);`,
		`INSERT INTO user_addresses (address_id, user_id) VALUES (4, 5);`,
	}

	log.SetLevel(log.WarningLevel)

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

func TestCompareDateGT(t *testing.T) {

	db, err := sql.Open("ramsql", "TestCompareDateGT")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE comp (dat DATE);")
	if err != nil {
		t.Fatalf("Cannot create table: %s", err)
	}

	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2018-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}
	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2019-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}
	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2020-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}

	query := "SELECT dat FROM comp WHERE dat > '2018-03-03'"

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var dat time.Time
		if err := rows.Scan(&dat); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		unwantedDate, err := time.Parse("2006-01-02", "2018-01-01")
		if err != nil {
			t.Fatal("Cannot parse unwanted date", err)
		}
		if dat.Equal(unwantedDate) {
			t.Fatalf("Unwanted row: %v", dat)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Unwanted number of rows :%d", nb)
	}

}

func TestCompareDateLT(t *testing.T) {

	db, err := sql.Open("ramsql", "TestCompareDateLT")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE comp (dat DATE);")
	if err != nil {
		t.Fatalf("Cannot create table: %s", err)
	}

	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2018-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}
	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2019-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}
	_, err = db.Exec("INSERT INTO comp (dat) VALUES ('2020-01-01')")
	if err != nil {
		t.Fatal("Cannot insert value")
	}

	query := "SELECT dat FROM comp WHERE dat < '2019-03-03'"

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var dat time.Time
		if err := rows.Scan(&dat); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		unwantedDate, err := time.Parse("2006-01-02", "2020-01-01")
		if err != nil {
			t.Fatal("Cannot parse unwanted date", err)
		}
		if dat.Equal(unwantedDate) {
			t.Fatalf("Unwanted row: %v", dat)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Unwanted number of rows %d", nb)
	}

}

func TestDate(t *testing.T) {

	query := `
	insert into "token" ("uuid","hash_token","user_id","expires")
	values ('a0db2f53-f668-472a-87e5-840f185128c2',
          'dj9cNdtipDBCBztYX9M0Qia0I7Ity9wlpfCAH+Xl33e9xAPBWxT+dsrt6/SAX32Z9Bt0sps1nIWF2/e7sh4tqg==',
          1,
          2015-09-10 14:03:09.444695269 +0200 CEST);`

	db, err := sql.Open("ramsql", "TestDate")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s", err)
	}
	defer db.Close()

	create := `create table if not exists "refresh_token" ("uuid" text not null primary key,
	"hash_token" text,
	"user_id" bigint,
	"expires" timestamp with time zone,
	"tag" text) ;`
	_, err = db.Exec(create)
	if err != nil {
		t.Fatalf("Cannot create table: %s", err)
	}

	_, err = db.Exec(`CREATE TABLE token (uuid TEXT PRIMARY KEY, hash_token TEXT, user_id BIGINT, expires TIMESTAMP WITH TIME ZONE)`)
	if err != nil {
		t.Fatalf("Cannot create table: %s", err)
	}

	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("Cannot insert data: %s", err)
	}

	var date time.Time
	err = db.QueryRow(`SELECT token.expires FROM token WHERE 1`).Scan(&date)
	if err != nil {
		t.Fatalf("Cannot select date: %s", err)
	}

	if fmt.Sprintf("%v", date) != "2015-09-10 14:03:09.444695269 +0200 CEST" {
		t.Fatalf("Expected specific date, got %v", date)
	}
}

func TestAnd(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestAnd")
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

	query := `SELECT * FROM user
			WHERE user.surname = Doe
			AND user.name = Jane`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && name != "Jane" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 1 {
		t.Fatalf("Expected 1 rows, got %d", nb)
	}

	query = `UPDATE user SET age = 31 WHERE name = $1 AND surname = $2`
	_, err = db.Exec(query, "Bruce", "Wayne")
	if err != nil {
		t.Fatalf("Cannot run UPDATE query with AND: %s\n", err)
	}

}

func TestEqualAndDistinct(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES ('Foo', 'Bar', 20);`,
		`INSERT INTO user (name, surname, age) VALUES ('John', 'Doe', 32);`,
		`INSERT INTO user (name, surname, age) VALUES ('Jane', 'Doe', 33);`,
		`INSERT INTO user (name, surname, age) VALUES ('Joe', 'Doe', 10);`,
		`INSERT INTO user (name, surname, age) VALUES ('Homer', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Marge', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Bruce', 'Wayne', 3333);`,
	}

	db, err := sql.Open("ramsql", "TestEqualAndDistinct")
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

	query := `SELECT * FROM user
			WHERE user.surname = 'Doe'
			AND user.name <> 'Jane'`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if name == "Jane" || surname != "Doe" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}
}

func TestGreaterThanOrEqualAndLessThanOrEqual(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestGreaterThanOrEqualAndLessThanOrEqual")
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

	query := `SELECT * FROM user
			WHERE user.age <= 40
			AND age >= 32`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if age < 32 || age > 40 {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 4 {
		t.Fatalf("Expected 4 rows, got %d", nb)
	}

}

func TestGreaterThanAndLessThan(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestGreaterThanAndLessThan")
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

	query := `SELECT * FROM user
			WHERE user.age < 40
			AND age > 25`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if age <= 25 || age >= 40 {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}

}

func TestOr(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	db, err := sql.Open("ramsql", "TestOr")
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

	query := `SELECT * FROM user
			WHERE user.name = Homer
			OR user.name = Marge`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age int
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Simpson" {
			t.Fatalf("Unwanted row: %s %s %d", name, surname, age)
		}

		nb++
	}

	if nb != 2 {
		t.Fatalf("Expected 2 rows, got %d", nb)
	}

}

func TestDefaultTimestamp(t *testing.T) {

	batch := []string{
		`CREATE TABLE pokemon (name TEXT, type TEXT, seen TIMESTAMP WITH TIME ZONE DEFAULT LOCALTIMESTAMP)`,
		`INSERT INTO pokemon (name, type) VALUES ('Charmander', 'fire')`,
	}

	db, err := sql.Open("ramsql", "TestDefaultTimestamp")
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

	query := `SELECT * FROM pokemon WHERE name = 'Charmander'`
	var name, montype string
	var seen time.Time
	err = db.QueryRow(query).Scan(&name, &montype, &seen)
	if err != nil {
		t.Fatalf("cannot load charmander row: %s\n", err)
	}

	if seen.IsZero() {
		t.Fatalf("expected localtimestamp, got 0")
	}

	query = `UPDATE pokemon SET seen = current_timestamp WHERE name = 'Charmander'`
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("cannot update timestamp: %s\n", err)
	}

	query = `SELECT seen FROM pokemon WHERE name = 'Charmander'`
	var seen2 time.Time
	err = db.QueryRow(query).Scan(&seen2)
	if err != nil {
		t.Fatalf("cannot load charmander last seen: %s\n", err)
	}

	if seen2.IsZero() {
		t.Fatalf("expected localtimestamp, got 0")
	}
	if seen2 == seen {
		t.Fatalf("expected different value after update (new %s vs old %s)", seen2, seen)
	}

	// Check with NOW()
	query = `UPDATE pokemon SET seen = NOW() WHERE name = 'Charmander'`
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("cannot update timestamp: %s\n", err)
	}

	query = `SELECT seen FROM pokemon WHERE name = 'Charmander'`
	var seen3 time.Time
	err = db.QueryRow(query).Scan(&seen3)
	if err != nil {
		t.Fatalf("cannot load charmander last seen after update: %s\n", err)
	}

	if seen3.IsZero() {
		t.Fatalf("expected localtimestamp, got 0")
	}
	if seen3 == seen2 {
		t.Fatalf("expected different value after update")
	}

	query = `INSERT INTO pokemon (name, type, seen) VALUES ('Squirtle', 'water', NOW())`
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("Cannot insert row using NOW(): %s\n", err)
	}
	query = `SELECT seen FROM pokemon WHERE name = 'Squirtle'`
	var s time.Time
	err = db.QueryRow(query).Scan(&s)
	if err != nil {
		t.Fatalf("cannot load new row: %s\n", err)
	}

	if s.IsZero() {
		t.Fatalf("expected localtimestamp, got 0")
	}
}

func TestOffset(t *testing.T) {

	batch := []string{
		`CREATE TABLE pokemon (name TEXT)`,
		`INSERT INTO pokemon (name) VALUES ('Charmander')`,
		`INSERT INTO pokemon (name) VALUES ('Bulbasaur')`,
		`INSERT INTO pokemon (name) VALUES ('Squirtle')`,
	}

	db, err := sql.Open("ramsql", "TestOffset")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: %s", err)
		}
	}

	query := `SELECT * FROM pokemon OFFSET 2`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			t.Fatalf("rows.Scan: %s", err)
		}
		count++
	}

	if count != 1 {
		t.Fatalf("Expected offset of 2 on 3 rows, got %d rows", count)
	}
}

func TestUnique(t *testing.T) {

	batch := []string{
		`CREATE TABLE pokemon (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL)`,
		`INSERT INTO pokemon (name) VALUES ('Charmander')`,
		`INSERT INTO pokemon (name) VALUES ('Bulbasaur')`,
		`INSERT INTO pokemon (name) VALUES ('Squirtle')`,
	}

	db, err := sql.Open("ramsql", "TestUnique")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: %s", err)
		}
	}

	query := `INSERT INTO pokemon (name) VALUES ('Charmander')`
	_, err = db.Exec(query)
	if err == nil {
		t.Fatalf("Expected error with UNIQUE violation")
	}
}

func TestJSON(t *testing.T) {

	batch := []string{
		`CREATE TABLE test (sequence_number BIGSERIAL PRIMARY KEY, data JSON)`,
		`INSERT INTO test (data) VALUES ('{"id":"c05d13bd-9d9b-4ea1-95f2-9b11ed3a7d38","name":"test"}')`,
	}

	db, err := sql.Open("ramsql", "TestJSON")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: %s", err)
		}
	}

	query := `SELECT data FROM test`
	var data string
	err = db.QueryRow(query).Scan(&data)
	if err != nil {
		t.Fatalf("sql.QueryRow: %s", err)
	}

	s := struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{}

	err = json.Unmarshal([]byte(data), &s)
	if err != nil {
		t.Fatalf("json.Unmarshal 1: %s", err)
	}
	if s.ID != "c05d13bd-9d9b-4ea1-95f2-9b11ed3a7d38" || s.Name != "test" {
		t.Fatalf("Unexpected values (first unmarshal): %+v\n", s)
	}

	query = `INSERT INTO test (data) VALUES ($1)`
	_, err = db.Exec(query, data)
	if err != nil {
		t.Fatalf("db.Exec: %s", err)
	}

	query = `SELECT data FROM test WHERE sequence_number = 2`
	err = db.QueryRow(query).Scan(&data)
	if err != nil {
		t.Fatalf("sql.QueryRow: %s", err)
	}

	err = json.Unmarshal([]byte(data), &s)
	if err != nil {
		t.Fatalf("json.Unmarshal 2: %s", err)
	}
	if s.ID != "c05d13bd-9d9b-4ea1-95f2-9b11ed3a7d38" || s.Name != "test" {
		t.Fatalf("Unexpected values (second unmarshal): %+v\n", s)
	}
}

func TestDistinct(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES ('Foo', 'Bar', 20);`,
		`INSERT INTO user (name, surname, age) VALUES ('John', 'Doe', 20);`,
		`INSERT INTO user (name, surname, age) VALUES ('Jane', 'Doe', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Joe', 'Doe', 10);`,
		`INSERT INTO user (name, surname, age) VALUES ('Homer', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Marge', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Bruce', 'Wayne', 3333);`,
	}

	db, err := sql.Open("ramsql", "TestDistinct")
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

	testDistinct := func(t *testing.T, query string, exp int, dest ...interface{}) {
		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("sql.Query: %s", err)
		}

		var got int
		for rows.Next() {
			got++
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}

		if got != exp {
			t.Fatalf("Expected %d rows, got %d", exp, got)
		}
	}

	t.Run("distinct", func(t *testing.T) {
		var surname string
		testDistinct(t, `SELECT DISTINCT surname FROM user`, 4, &surname)
	})
	t.Run("distinct-on", func(t *testing.T) {
		var name string
		testDistinct(t, `SELECT DISTINCT ON (surname) name FROM user ORDER BY surname, age DESC`, 4, &name)
	})
}

func TestBracketWhereClause(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES ('Foo', 'Bar', 20);`,
		`INSERT INTO user (name, surname, age) VALUES ('John', 'Doe', 32);`,
		`INSERT INTO user (name, surname, age) VALUES ('Jane', 'Doe', 33);`,
		`INSERT INTO user (name, surname, age) VALUES ('Joe', 'Doe', 10);`,
		`INSERT INTO user (name, surname, age) VALUES ('Homer', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Marge', 'Simpson', 40);`,
		`INSERT INTO user (name, surname, age) VALUES ('Bruce', 'Wayne', 3333);`,
	}

	db, err := sql.Open("ramsql", "TestBracketWhereClause")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `SELECT * FROM "user" WHERE (age < 40)`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}
	defer rows.Close()
}

func TestInsertByteArray(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertByteArray")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	_, err = db.Exec(`CREATE TABLE test (sequence_number BIGSERIAL PRIMARY KEY, json JSON, created_at TIMESTAMP DEFAULT NOW())`)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	j, _ := json.Marshal(map[string]string{"a": "a"})
	_, err = db.Exec("INSERT INTO test (json) values ($1)", j)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	var s string
	err = db.QueryRow("SELECT json FROM test WHERE 1 = 1 limit 1").Scan(&s)
	if err != nil {
		t.Fatalf("sql.Select: Error: %s\n", err)
	}

	if s != string(j) {
		t.Fatalf("Expected JSON to be '%s', got '%s'", string(j), s)
	}
}

func TestInsertByteArrayODBC(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertByteArrayODBC")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	_, err = db.Exec(`CREATE TABLE test_json (sequence_number BIGSERIAL PRIMARY KEY, json JSON, created_at TIMESTAMP DEFAULT NOW())`)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	j, _ := json.Marshal(map[string]string{"a": "a"})

	_, err = db.Exec("INSERT INTO test_json (json) values (?)", j)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	var s []byte
	err = db.QueryRow("SELECT json FROM test_json limit 1").Scan(&s)
	if err != nil {
		t.Fatalf("sql.Select: Error: %s\n", err)
	}

	if !reflect.DeepEqual(s, j) {
		t.Fatalf("Expected JSON to be '%s', got '%s'", j, s)
	}
}

func TestSchema(t *testing.T) {

	db, err := sql.Open("ramsql", "TestSchema")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	_, err = db.Exec(`CREATE TABLE foo.bar (baz TEXT)`)
	if err == nil {
		t.Fatalf("expected error trying to create a table on non-existent schema")
	}

	_, err = db.Exec(`CREATE TABLE "foo"."bar" ()`)
	if err == nil {
		t.Fatalf("expected error trying to create a table on non-existent schema")
	}

	_, err = db.Exec(`CREATE TABLE "foo"."bar" (baz TEXT)`)
	if err == nil {
		t.Fatalf("expected error trying to create a table on non-existent schema")
	}

	_, err = db.Exec(`CREATE SCHEMA "foo"`)
	if err != nil {
		t.Fatalf("unexpected error trying to create a schema: %s", err)
	}

	_, err = db.Exec(`CREATE TABLE "foo"."bar" (baz TEXT)`)
	if err != nil {
		t.Fatalf("unexpected error trying to create a table on existing schema: %s", err)
	}

	_, err = db.Exec(`INSERT INTO "foo"."bar" (baz) VALUES ("yep")`)
	if err != nil {
		t.Fatalf("unexpected error trying to insert row in a table with existing schema: %s", err)
	}

	var baz string
	err = db.QueryRow(`SELECT baz FROM "bar" WHERE 1`).Scan(&baz)
	if err == nil {
		t.Fatalf("expected error fetching row from table existing in another schema")
	}

	err = db.QueryRow(`SELECT baz FROM "foo"."bar" WHERE 1`).Scan(&baz)
	if err != nil {
		t.Fatalf("unexpected error fetching row from table in existing schema: %s", err)
	}

	if baz != "yep" {
		t.Fatalf("expected baz value to be 'yep', got '%s'", baz)
	}

	_, err = db.Exec(`DROP SCHEMA nope`)
	if err == nil {
		t.Fatalf("expected error dropping non-existing schema")
	}

	_, err = db.Exec(`DROP SCHEMA foo`)
	if err != nil {
		t.Fatalf("unexpected error dropping existing schema: %s", err)
	}
}

func TestFloat(t *testing.T) {

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age float(8));`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20.0);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32.0);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33.0939959238);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 1e-10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Future, -3333);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Demo, -1.0);`,
	}

	db, err := sql.Open("ramsql", "TestFloat")
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

	query := `SELECT * FROM user
			WHERE user.surname = Doe
			AND user.name = Joe`

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("sql.Query: %s", err)
	}

	var nb int
	for rows.Next() {
		var name, surname string
		var age float64
		if err := rows.Scan(&name, &surname, &age); err != nil {
			t.Fatalf("Cannot scan row: %s", err)
		}
		if surname != "Doe" && name != "Jane" {
			t.Fatalf("Unwanted row: %s %s %f", name, surname, age)
		}

		nb++
	}

	if nb != 1 {
		t.Fatalf("Expected 1 rows, got %d", nb)
	}

	query = `UPDATE user SET age = $3 WHERE name = $1 AND surname = $2`
	var age float64 = 3450000000000

	t.Logf("age in scientfic notation is 3.45e+12: %v", age)

	_, err = db.Exec(query, "Bruce", "Wayne", age)
	if err != nil {
		t.Fatalf("Cannot run UPDATE query with AND: %s\n", err)
	}

	err = db.QueryRow(`SELECT age FROM user WHERE surname = 'Demo'`).Scan(&age)
	if err != nil {
		t.Fatalf("Cannot load negative age: %s", err)
	}
	if age != -1.0 {
		t.Fatalf("Expected age to be -1.0, got %f", age)
	}
}

func TestDrop(t *testing.T) {
	db, err := sql.Open("ramsql", "TestDrop")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
	if err != nil {
		t.Fatalf("%s", err)
	}

	_, err = db.Exec("DROP TABLE account")
	if err != nil {
		t.Fatalf("cannot drop table: %s", err)
	}
}

func TestTrunc(t *testing.T) {
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

func TestInsertSingle(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertASingle")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT, funny BOOLEAN)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO cat (breed, name, funny) VALUES ('indeterminate', 'Uhura', false)")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("Expected to affect 1 row, affected %v", rowsAffected)
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot check last inserted ID: %s", err)
	}

	row := db.QueryRow("SELECT breed, name, funny FROM cat WHERE id = ?", insertedId)
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var breed string
	var name string
	var funny bool = true

	err = row.Scan(&breed, &name, &funny)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if breed != "indeterminate" || name != "Uhura" {
		t.Fatalf("Expected breed 'indeterminate' and name 'Uhura', got breed '%v' and name '%v'", breed, name)
	}

	if funny != false {
		t.Fatalf("Expected funny=false, got true")
	}

	_, err = db.Exec("INSERT INTO cat (breed, name, funny) VALUES ('indeterminate', 'Kuhura', true)")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	err = db.QueryRow("SELECT breed, name, funny FROM cat WHERE name = ?", "Kuhura").Scan(&breed, &name, &funny)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if funny != true {
		t.Fatalf("Expected funny=true, got false")
	}

}

func TestInsertSingleReturning(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertSingleReturning")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	rows, err := db.Query("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Nala') RETURNING id")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}
	defer rows.Close()

	hasRow := rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id 1, got id %v", id)
	}

	hasRow = rows.Next()
	if hasRow {
		t.Fatalf("Returned more than one row: %s", err)
	}
}

func TestInsertSingleReturningUint(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertSingleReturningUint")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id BIGSERIAL PRIMARY KEY, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	rows, err := db.Query("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Nala') RETURNING id")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}
	defer rows.Close()

	hasRow := rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	var id uint
	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id 1, got id %v", id)
	}

	hasRow = rows.Next()
	if hasRow {
		t.Fatalf("Returned more than one row: %s", err)
	}
}

func TestInsertWithMissingValue(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertWithMissingValue")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE user (
			id INT AUTOINCREMENT,
			email TEXT DEFAULT 'example@example.com',
			name TEXT
		)
	`)
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO user (name) VALUES ('Bob')")
	if err != nil {
		t.Fatalf("Cannot insert into table user: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("Expected to affect 1 row, affected %v", rowsAffected)
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Cannot check last inserted ID: %s", err)
	}

	row := db.QueryRow("SELECT email, name FROM user WHERE id = ?", insertedId)
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var email string
	var name string
	err = row.Scan(&email, &name)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if email != "example@example.com" || name != "Bob" {
		t.Fatalf("Expected email 'example@example.com' and name 'Bob', got email '%v' and name '%v'", email, name)
	}
}

func TestInsertMultiple(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertMultiple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	result, err := db.Exec("INSERT INTO cat (breed, name) VALUES ('persian', 'Mozart'), ('persian', 'Danton')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check rows affected: %s", err)
	}
	if rowsAffected != 2 {
		t.Fatalf("Expected to affect 2 rows, affected %v", rowsAffected)
	}
}

func TestInsertMultipleReturning(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInsertMultipleReturning")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE cat (id INT AUTOINCREMENT, breed TEXT, name TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	rows, err := db.Query("INSERT INTO cat (breed, name) VALUES ('indeterminate', 'Spock'), ('indeterminate', 'Belanna') RETURNING id")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}
	defer rows.Close()

	hasRow := rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	var id int
	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if id != 1 {
		t.Fatalf("Expected id 1, got id %v", id)
	}

	hasRow = rows.Next()
	if !hasRow {
		t.Fatalf("Did not return a row: %s", err)
	}

	err = rows.Scan(&id)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if id != 2 {
		t.Fatalf("Expected id 2, got id %v", id)
	}

	hasRow = rows.Next()
	if hasRow {
		t.Fatalf("Returned more than two rows: %s", err)
	}
}

func TestJoinOrderBy(t *testing.T) {

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

func TestMultipleJoin(t *testing.T) {

	db, err := sql.Open("ramsql", "TestMultipleJoin")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE user (id BIGSERIAL, name TEXT)`,
		`CREATE TABLE address (id BIGSERIAL, user_id INT, value TEXT)`,
		`CREATE TABLE user_group (id BIGSERIAL, user_id INT, name TEXT)`,
		`INSERT INTO user (name) VALUES ($$riri$$)`,
		`INSERT INTO user (name) VALUES ($$fifi$$)`,
		`INSERT INTO user (name) VALUES ($$loulou$$)`,
		`INSERT INTO user (name) VALUES ("foo")`,
		`INSERT INTO user (name) VALUES ("bar")`,
		`INSERT INTO user (name) VALUES ("baz")`,
		`INSERT INTO address (user_id, value) VALUES (1, 'rue du puit')`,
		`INSERT INTO address (user_id, value) VALUES (1, 'rue du désert')`,
		`INSERT INTO address (user_id, value) VALUES (3, 'rue du chemin')`,
		`INSERT INTO address (user_id, value) VALUES (2, 'boulevard du con')`,
		`INSERT INTO address (user_id, value) VALUES (2, 'boulevard du fion')`,
		`INSERT INTO address (user_id, value) VALUES (2, 'boulevard du rond')`,
		`INSERT INTO address (user_id, value) VALUES (3, 'boulevard du don')`,
		`INSERT INTO address (user_id, value) VALUES (4, 'boulevard du son')`,
		`INSERT INTO address (user_id, value) VALUES (5, 'boulevard du mont')`,
		`INSERT INTO address (user_id, value) VALUES (6, 'boulevard du non')`,
		`INSERT INTO user_group (user_id, name) VALUES (1, 'toto')`,
		`INSERT INTO user_group (user_id, name) VALUES (2, 'toto')`,
		`INSERT INTO user_group (user_id, name) VALUES (3, 'lonely')`,
		`INSERT INTO user_group (user_id, name) VALUES (1, 'cowboy')`,
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
			JOIN user_group ON user_group.user_id = address.user_id
			WHERE user_group.name = $1
			ORDER BY address.value ASC`
	rows, err := db.Query(query, "toto")
	if err != nil {
		t.Fatalf("Cannot select with 3 tables joined: %s", err)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		n++
	}
	if n != 5 {
		t.Fatalf("Expected 5 rows, got %d", n)
	}

}

func TestJoinGroup(t *testing.T) {

	db, err := sql.Open("ramsql", "TestJoinGroup")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	init := []string{
		`CREATE TABLE user (id BIGSERIAL, name TEXT)`,
		`CREATE TABLE group (id BIGSERIAL, name TEXT)`,
		`CREATE TABLE user_group (id BIGSERIAL, user_id INT, group_id INT)`,
		`INSERT INTO user (name) VALUES ($$riri$$)`,
		`INSERT INTO user (name) VALUES ($$fifi$$)`,
		`INSERT INTO user (name) VALUES ($$loulou$$)`,
		`INSERT INTO group (name) VALUES ('cowboys')`,
		`INSERT INTO group (name) VALUES ('troopers')`,
		`INSERT INTO group (name) VALUES ('toys')`,
		`INSERT INTO user_group (user_id, group_id) VALUES (1, 1)`,
		`INSERT INTO user_group (user_id, group_id) VALUES (2, 1)`,
	}
	for _, q := range init {
		_, err := db.Exec(q)
		if err != nil {
			t.Fatalf("Cannot initialize test: %s", err)
		}
	}

	query := `SELECT user.name FROM user
			JOIN user_group ON user.id = user_group.user_id
			WHERE user_group.group_id = $1
			ORDER BY user.name ASC`
	rows, err := db.Query(query, 1)
	if err != nil {
		t.Fatalf("Cannot select joined: %s", err)
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

func TestOrderByInt(t *testing.T) {

	db, err := sql.Open("ramsql", "TestOrderByInt")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE surname = Wayne OR surname = Doe ORDER BY age DESC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}
	defer rows.Close()

	var age, last, size int64
	last = 4000
	// Now every time 'age' should be less than 'last'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if age > last {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 4 {
		t.Fatalf("Expecting 4 rows here, got %d", size)
	}

	query = `SELECT age FROM user ORDER BY age ASC`
	rows, err = db.Query(query)
	if err != nil {
		t.Fatalf("cannot order by age: %s\n", err)
	}

	size = 0
	last = 0
	// Now 'last' should be less than current 'age'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if last > age {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 7 {
		t.Fatalf("Expecting 7 rows, got %d", size)
	}
}

func TestOrderByString(t *testing.T) {

	db, err := sql.Open("ramsql", "TestOrderByString")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT name, surname FROM user WHERE surname = $1 ORDER BY name ASC`
	rows, err := db.Query(query, "Doe")
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}

	var names []string
	var name, surname string
	for rows.Next() {
		err = rows.Scan(&name, &surname)
		if err != nil {
			t.Fatalf("Cannot scan row: %s\n", err)
		}
		if surname != "Doe" {
			t.Fatalf("Didn't expect surname being %s", surname)
		}
		names = append(names, name)
	}

	if len(names) != 3 {
		t.Fatalf("Expected 3 rows, not %d", len(names))
	}

	if names[0] != "Jane" {
		t.Fatalf("Wanted Jane, got %s", names[0])
	}

	if names[1] != "Joe" {
		t.Fatalf("Wanted Joe, got %s", names[1])
	}

	if names[2] != "John" {
		t.Fatalf("Wanted John, got %s", names[2])
	}

}

func TestOrderByLimit(t *testing.T) {

	db, err := sql.Open("ramsql", "TestOrderByLimit")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
		`INSERT INTO user (name, surname, age) VALUES (Foo, Bar, 20);`,
		`INSERT INTO user (name, surname, age) VALUES (John, Doe, 32);`,
		`INSERT INTO user (name, surname, age) VALUES (Jane, Doe, 33);`,
		`INSERT INTO user (name, surname, age) VALUES (Joe, Doe, 10);`,
		`INSERT INTO user (name, surname, age) VALUES (Homer, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Marge, Simpson, 40);`,
		`INSERT INTO user (name, surname, age) VALUES (Bruce, Wayne, 3333);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE 1=1 ORDER BY age DESC LIMIT 2`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot limit select: %s", err)
	}

	var age, last, size int64
	last = 4000
	// Now every time 'age' should be less than 'last'
	for rows.Next() {
		err = rows.Scan(&age)
		if err != nil {
			t.Fatalf("Cannot scan age: %s", err)
		}
		if age > last {
			t.Fatalf("Got %d previously and now %d", last, age)
		}
		last = age
		size++
	}

	if size != 2 {
		t.Fatalf("Expecting 2 rows here, got %d", size)
	}
}

func TestOrderByIntEmpty(t *testing.T) {

	db, err := sql.Open("ramsql", "TestOrderByIntEmpty")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age INT);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT age FROM user WHERE surname = Wayne OR surname = Doe ORDER BY age DESC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}
	defer rows.Close()

}

func TestOrderByMultipleStrings(t *testing.T) {

	db, err := sql.Open("ramsql", "TestOrderByMultipleStrings")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	batch := []string{
		`CREATE TABLE user (name TEXT, surname TEXT, age BIGINT, savings DECIMAL);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Angel, 11, 125.215);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Angel, 11, 1.1);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Joe, Zebra, 32, 0.921);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Anna, Angel, 33, 0);`,
		`INSERT INTO user (name, surname, age, savings) VALUES (Anna, Zebra, 9, 25);`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s", err)
		}
	}

	query := `SELECT name, surname, age, savings FROM user ORDER BY name ASC, age DESC, savings ASC`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Cannot select and order by age: %s", err)
	}

	exp := [][]string{
		{"Anna", "33", "0"},
		{"Anna", "9", "25"},
		{"Joe", "32", "0.921"},
		{"Joe", "11", "1.1"},
		{"Joe", "11", "125.215"},
	}
	var (
		got           [][]string
		name, surname string
		age           int64
		savings       float64
	)
	for rows.Next() {
		err = rows.Scan(&name, &surname, &age, &savings)
		if err != nil {
			t.Fatalf("Cannot scan row: %s\n", err)
		}
		got = append(got, []string{name, fmt.Sprint(age), fmt.Sprint(savings)})
	}

	if len(exp) != len(got) {
		t.Fatalf("length mismatch, expected %d but got %d", len(exp), len(got))
	}
	for i := 0; i < len(exp); i++ {
		for j := 0; j < len(exp[i]); j++ {
			if exp[i][j] != got[i][j] {
				t.Fatalf("data mismatch at %d/%d, expected %v but got %v", i, j, exp[i][j], got[i][j])
			}
		}
	}
}

func TestSelectNoOp(t *testing.T) {
	log.SetLevel(log.WarningLevel)

	db, err := sql.Open("ramsql", "TestSelectNoOp")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE account (id BIGSERIAL, email TEXT)`,
		`INSERT INTO account (email) VALUES ("foo@bar.com")`,
		`INSERT INTO account (email) VALUES ("bar@bar.com")`,
		`INSERT INTO account (email) VALUES ("foobar@bar.com")`,
		`INSERT INTO account (email) VALUES ("babar@bar.com")`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	query := `SELECT * from account WHERE 1 = 1`
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	nb := 0
	for rows.Next() {
		nb++
	}

	if nb != 4 {
		t.Fatalf("Expected 4 rows, got %d", nb)
	}

}

func TestSelect(t *testing.T) {
	db, err := sql.Open("ramsql", "TestSelect")
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

	rows, err := db.Query("SELECT * FROM account WHERE email = $1", "foo@bar.com")
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

	row := db.QueryRow("SELECT * FROM account WHERE email = $1", "foo@bar.com")
	if row == nil {
		t.Fatalf("sql.QueryRow error")
	}

	var email string
	var id int
	err = row.Scan(&id, &email)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if id != 1 {
		t.Fatalf("Expected id = 1, got %d", id)
	}

	if email != "foo@bar.com" {
		t.Fatalf("Expected email = <foo@bar.com>, got <%s>", email)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("sql.Close : Error : %s\n", err)
	}

}

func TestCount(t *testing.T) {
	db, err := sql.Open("ramsql", "TestCount")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	batch := []string{
		`CREATE TABLE account (id BIGSERIAL, email TEXT)`,
		`INSERT INTO account (email) VALUES ("foo@bar.com")`,
		`INSERT INTO account (email) VALUES ("bar@bar.com")`,
		`INSERT INTO account (email) VALUES ("foobar@bar.com")`,
		`INSERT INTO account (email) VALUES ("babar@bar.com")`,
	}

	for _, b := range batch {
		_, err = db.Exec(b)
		if err != nil {
			t.Fatalf("sql.Exec: Error: %s\n", err)
		}
	}

	var count int64
	err = db.QueryRow(`SELECT COUNT(*) FROM account WHERE 1=1`).Scan(&count)
	if err != nil {
		t.Fatalf("cannot select COUNT of account: %s\n", err)
	}

	if count != 4 {
		t.Fatalf("Expected count to be 4, not %d", count)
	}

	err = db.QueryRow(`SELECT COUNT(i_dont_exist_lol) FROM account WHERE 1=1`).Scan(&count)
	if err == nil {
		t.Fatalf("Expected an error from a non existing attribute")
	}

}

func TestUpdateSimple(t *testing.T) {

	db, err := sql.Open("ramsql", "TestUpdateSimple")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("UPDATE account SET email = 'roger@gmail.com' WHERE id = 2")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	row := db.QueryRow("SELECT * FROM account WHERE id = 2")
	if row == nil {
		t.Fatalf("sql.Query failed")
	}

	var email string
	var id int
	err = row.Scan(&id, &email)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}

	if email != "roger@gmail.com" {
		t.Fatalf("Expected email 'roger@gmail.com', got '%s'", email)
	}
}

func TestUpdateIsNull(t *testing.T) {

	db, err := sql.Open("ramsql", "TestUpdateIsNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE DEFAULT NULL)")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	res, err := db.Exec("UPDATE account SET email = 'roger@gmail.com', creation_date = $1 WHERE id = 2 AND creation_date IS NULL", time.Now())
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	ra, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("Cannot check number of rows affected: %s", err)
	}
	if ra != 1 {
		t.Fatalf("Expected 1 row, affected. Got %d", ra)
	}

	rows, err := db.Query(`SELECT id FROM account WHERE creation_date IS NULL`)
	if err != nil {
		t.Fatalf("cannot select null columns: %s", err)
	}

	var n, id int64
	for rows.Next() {
		n++
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("cannot scan null columns: %s", err)
		}
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("Expected 1 rows, got %d", n)
	}

	rows, err = db.Query(`SELECT id FROM account WHERE creation_date IS NOT NULL`)
	if err != nil {
		t.Fatalf("cannot select not null columns: %s", err)
	}

	n = 0
	for rows.Next() {
		n++
		err = rows.Scan(&id)
		if err != nil {
			t.Fatalf("cannot scan null columns: %s", err)
		}
	}
	rows.Close()
	if n != 1 {
		t.Fatalf("Expected 1 rows, got %d", n)
	}

}

func TestUpdateNotNull(t *testing.T) {

	db, err := sql.Open("ramsql", "TestUpdateNotNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE DEFAULT NOW())")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('leon@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	_, err = db.Exec("UPDATE account SET email = 'roger@gmail.com' WHERE id = 2 AND creation_date IS NOT NULL")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

}

func TestUpdateToNull(t *testing.T) {

	db, err := sql.Open("ramsql", "TestUpdateToNull")
	if err != nil {
		t.Fatalf("sql.Open : Error : %s\n", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE account (id INT AUTOINCREMENT, email TEXT, creation_date TIMESTAMP WITH TIME ZONE DEFAULT NOW())")
	if err != nil {
		t.Fatalf("sql.Exec: Error: %s\n", err)
	}

	_, err = db.Exec("INSERT INTO account ('email') VALUES ('foo@bar.com')")
	if err != nil {
		t.Fatalf("Cannot insert into table account: %s", err)
	}

	row1 := db.QueryRow("SELECT email FROM account WHERE id = 1")
	if row1 == nil {
		t.Fatalf("sql.Query failed")
	}

	var email1 *string
	err = row1.Scan(&email1)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if email1 == nil {
		t.Fatalf("expected 'foo@bar.com' email, but got NULL")
	}

	_, err = db.Exec("UPDATE account SET email = NULL WHERE id = 1")
	if err != nil {
		t.Fatalf("Cannot update table account: %s", err)
	}

	row2 := db.QueryRow("SELECT email FROM account WHERE id = 1")
	if row2 == nil {
		t.Fatalf("sql.Query failed")
	}

	var email2 sql.NullString
	err = row2.Scan(&email2)
	if err != nil {
		t.Fatalf("row.Scan: %s", err)
	}
	if email2.Valid == true {
		t.Fatalf("expected NULL email, but got '%v'", email2.String)
	}

}

func TestDeadlock(t *testing.T) {
	db, err := sql.Open("ramsql", "TestDeadlock")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Fatalf("cannot ping db: %s", err)
	}
	if _, err := db.Exec("CREATE TABLE address (id int, street text)"); err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	stmt, err := db.Prepare("insert into address (id,street) values (?,?)")
	if err != nil {
		t.Fatalf("cannot prepare statement: %s", err)
	}

	for i := 0; i < 10; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("%d park ave", 100+i)); err != nil { // ERROR
			t.Fatalf("cannot exec %dnt statement: %s", i, err)
		}
	}
}

func TestNamedArg(t *testing.T) {

	db, err := sql.Open("ramsql", "TestNamedArg2")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}
	defer db.Close()

	var name string

	_, err = db.Exec(`CREATE TABLE people (id BIGSERIAL PRIMARY KEY, name TEXT, surname TEXT, age INT)`)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	_, err = db.ExecContext(context.TODO(), "INSERT INTO people (id,name,surname,age) VALUES (?,?,?,?)", 1234, "Ramone", "Juz", 12)
	if err != nil {
		t.Fatalf("cannot exec context: %s", err)
	}

	err = db.QueryRowContext(context.TODO(), "select p.name from people as p where p.id = :id;", sql.Named("id", 1234)).Scan(&name)
	if err != nil {
		t.Fatalf("cannot query context: %s", err)
	}

}

func TestScanTimestamp(t *testing.T) {
	db, err := sql.Open("ramsql", "TestScanTimestamp")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}
	defer db.Close()

	query := `
	  CREATE TABLE IF NOT EXISTS properties (
			id         VARCHAR(36)  PRIMARY KEY,
			street     VARCHAR(255),
			city       VARCHAR(32),
			state      VARCHAR(32),
			zip        VARCHAR(10),
			created_at TIMESTAMPTZ
		)
	`
	_, err = db.Exec(query)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	id := "OIJOIJ"
	query = `INSERT INTO properties (id, street, city, state, zip, created_at) VALUES ($1, $2, $3, $4, $5, NOW())`
	_, err = db.Exec(query, id, "5th", "NY", "NY", "NY")
	if err != nil {
		t.Fatalf("cannot insert into table: %s", err)
	}

	p := struct {
		ID        string
		Street    string
		City      string
		StateCode string
		Zip       string
		CreatedAt time.Time
	}{}

	query = `
		SELECT id, street, city, state, zip, created_at
		FROM properties WHERE id = $1
	`

	ctx := context.Background()
	err = db.QueryRowContext(ctx, query, id).Scan(
		&p.ID, &p.Street, &p.City,
		&p.StateCode, &p.Zip, &p.CreatedAt,
	)
	if err != nil {
		t.Fatalf("cannot scan in struct: %s", err)
	}
	if p.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt should not be 0")
	}
}

func TestPrimaryKeyConstraint(t *testing.T) {

	db, err := sql.Open("ramsql", "TestPrimaryKeyConstraint")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE test (a INT, b TEXT, PRIMARY KEY(a, b))`)
	if err != nil {
		t.Fatalf("Create table: %s", err)
	}
	_, err = db.Exec(`INSERT INTO test (a,b) VALUES (1, "1")`)
	if err != nil {
		t.Fatalf("Insert : %s", err)
	}
	_, err = db.Exec(`INSERT INTO test (a,b) VALUES (2, "2")`)
	if err != nil {
		t.Fatalf("Insert : %s", err)
	}
	_, err = db.Exec(`INSERT INTO test (a,b) VALUES (2, "1")`)
	if err != nil {
		t.Fatalf("Insert : %s", err)
	}
	_, err = db.Exec(`INSERT INTO test (a,b) VALUES (2, "1")`)
	if err == nil {
		t.Fatalf("expected error on primary key violation")
	}
}

func TestLimitEmptyTable(t *testing.T) {

	db, err := sql.Open("ramsql", "TestLimitEmptyTable")
	if err != nil {
		t.Fatalf("cannot open db: %s", err)
	}

	q := `CREATE TABLE event_db_entries (test_id BIGINT, result TEXT)`
	_, err = db.Exec(q)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	q = `SELECT * FROM "event_db_entries" WHERE 1 LIMIT 1`
	_, err = db.Query(q)
	if err != nil {
		t.Fatalf("cannot select: %s", err)
	}
}

func TestInformationSchema(t *testing.T) {

	db, err := sql.Open("ramsql", "TestInformationSchema")
	if err != nil {
		t.Fatalf("sql.Open: %s", err)
	}
	defer db.Close()

	// Create a couple of tables
	_, err = db.Exec("CREATE TABLE users (id INT, name TEXT)")
	if err != nil {
		t.Fatalf("cannot create users table: %s", err)
	}

	_, err = db.Exec("CREATE TABLE posts (id INT, title TEXT, user_id INT)")
	if err != nil {
		t.Fatalf("cannot create posts table: %s", err)
	}

	// Query information_schema.tables to verify the tables are registered
	rows, err := db.Query(`SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name`)
	if err != nil {
		t.Fatalf("cannot query information_schema.tables: %s", err)
	}
	defer rows.Close()

	// Expected tables: posts, users (alphabetically ordered)
	expected := []struct {
		schema    string
		name      string
		tableType string
	}{
		{"public", "posts", "BASE TABLE"},
		{"public", "users", "BASE TABLE"},
	}

	i := 0
	for rows.Next() {
		var schema, name, tableType string
		if err := rows.Scan(&schema, &name, &tableType); err != nil {
			t.Fatalf("cannot scan row: %s", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: %s.%s", schema, name)
		}

		if schema != expected[i].schema || name != expected[i].name || tableType != expected[i].tableType {
			t.Errorf("row %d: got (%s, %s, %s), want (%s, %s, %s)",
				i, schema, name, tableType,
				expected[i].schema, expected[i].name, expected[i].tableType)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("expected %d rows, got %d", len(expected), i)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows error: %s", err)
	}
}
