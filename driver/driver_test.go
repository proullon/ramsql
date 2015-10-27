package ramsql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/proullon/ramsql/engine/log"
)

func TestCreateTable(t *testing.T) {
	log.UseTestLogger(t)

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

func TestInsertEmptyString(t *testing.T) {
	log.UseTestLogger(t)

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

func TestInsertTable(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestInsertTable")
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
	log.UseTestLogger(t)
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

func TestSelectWhereAttribute(t *testing.T) {
	log.UseTestLogger(t)
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

	log.SetLevel(log.DebugLevel)
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

func TestSelectSimplePredicate(t *testing.T) {
	log.UseTestLogger(t)
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

func TestDelete(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestDelete")
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

func TestCount(t *testing.T) {
	log.UseTestLogger(t)
	db, err := sql.Open("ramsql", "TestCount")
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

	rows, err := db.Query("SELECT COUNT(*) FROM account WHERE 1")
	if err != nil {
		t.Fatalf("sql.Query error : %s", err)
	}

	if !rows.Next() {
		t.Fatal("No rows :(")
	}

	var count int64
	rows.Scan(&count)
	if count != 2 {
		t.Fatalf("Expected count = 2, got %d", count)
	}
}

func TestMultipleCreate(t *testing.T) {
	log.UseTestLogger(t)
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
	log.UseTestLogger(t)

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
	log.UseTestLogger(t)

	batch := []string{
		`CREATE TABLE address (id BIGSERIAL PRIMARY KEY, street TEXT, street_number INT);`,
		`CREATE TABLE user_addresses (address_id INT, user_id INT);`,
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

func TestDate(t *testing.T) {
	log.UseTestLogger(t)

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

	log.SetLevel(log.DebugLevel)
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
	log.UseTestLogger(t)

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

	log.SetLevel(log.DebugLevel)
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

func TestGreaterThanAndLessThan(t *testing.T) {
	log.UseTestLogger(t)

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

	log.SetLevel(log.DebugLevel)
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
	log.UseTestLogger(t)

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

	log.SetLevel(log.DebugLevel)
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
