package ramsql

import (
	"database/sql/driver"
	"testing"
)

func TestNumInputQuestionMarker(t *testing.T) {

	// Create a new stub Conn
	c := &Conn{}

	stmt := prepareStatement(c, "SELECT * FROM account WHERE email = '?'")
	if stmt == nil {
		t.Fatal("prepareStatement should not return nil")
	}

	if stmt.numInput != 1 {
		t.Fatalf("prepareStatement expected 1 input, got %d", stmt.numInput)
	}
}

func TestNumInputPostgreMarker(t *testing.T) {

	// Create a new stub Conn
	c := &Conn{}

	stmt := prepareStatement(c, "SELECT * FROM account WHERE email = '$1' AND foo = $2 LIMIT $2")
	if stmt == nil {
		t.Fatal("prepareStatement should not return nil")
	}

	if stmt.numInput != 2 {
		t.Fatalf("prepareStatement expected 2 input, got %d", stmt.numInput)
	}
}

func TestReplaceArgument(t *testing.T) {
	query := `SELECT * FROM account WHERE email = $1`
	wantedQuery := `SELECT * FROM account WHERE email = $$foo@bar.com$$`
	args := []driver.Value{
		driver.Value("foo@bar.com"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func TestReplaceTwoArguments(t *testing.T) {
	query := `SELECT * FROM account WHERE email = $1 AND password = $2`
	wantedQuery := `SELECT * FROM account WHERE email = $$foo@bar.com$$ AND password = $$ewfewgwewggew$$`
	args := []driver.Value{
		driver.Value("foo@bar.com"),
		driver.Value("ewfewgwewggew"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func TestReplaceTwoArgumentsTwice(t *testing.T) {
	query := `SELECT * FROM account WHERE email = $1 OR
	 email_backup = $1 AND password = $2 OR foo = $2`
	wantedQuery := `SELECT * FROM account WHERE email = $$foo@bar.com$$ OR
	 email_backup = $$foo@bar.com$$ AND password = $$ewfewgwewggew$$ OR foo = $$ewfewgwewggew$$`
	args := []driver.Value{
		driver.Value("foo@bar.com"),
		driver.Value("ewfewgwewggew"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func TestReplaceEnd(t *testing.T) {
	query := `SELECT * FROM account WHERE email = $1`
	wantedQuery := `SELECT * FROM account WHERE email = $$foo@bar.com$$`
	args := []driver.Value{
		driver.Value("foo@bar.com"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func TestReplaceEndODBC(t *testing.T) {
	query := `SELECT * FROM account WHERE email = ?`
	wantedQuery := `SELECT * FROM account WHERE email = $$foo@bar.com$$`
	args := []driver.Value{
		driver.Value("foo@bar.com"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func TestReplaceALot(t *testing.T) {
	query := `INSERT INTO "human" ("id","birth_date","first_name","middle_name","last_name","size","weight","gender","version","goal","hair_color","foo","bar","team") values ($1, $2, $3, $4, $5, $6, $7, $8, $9 ,$10,$11,$12,$13,$14)`

	wantedQuery := `INSERT INTO "human" ("id","birth_date","first_name","middle_name","last_name","size","weight","gender","version","goal","hair_color","foo","bar","team") values ($$1$$, $$2$$, $$3$$, $$4$$, $$5$$, $$6$$, $$7$$, $$8$$, $$9$$ ,$$10$$,$$11$$,$$12$$,$$13$$,$$14$$)`

	args := []driver.Value{
		driver.Value("1"),
		driver.Value("2"),
		driver.Value("3"),
		driver.Value("4"),
		driver.Value("5"),
		driver.Value("6"),
		driver.Value("7"),
		driver.Value("8"),
		driver.Value("9"),
		driver.Value("10"),
		driver.Value("11"),
		driver.Value("12"),
		driver.Value("13"),
		driver.Value("14"),
	}

	testReplaceArguments(t, query, args, wantedQuery)
}

func testReplaceArguments(t *testing.T, query string, args []driver.Value, wantedQuery string) {
	finalQuery := replaceArguments(query, args)
	if finalQuery != wantedQuery {
		t.Fatalf("Expected <%s>, got <%s>", wantedQuery, finalQuery)
	}
}
