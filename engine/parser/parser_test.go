package parser

import (
	"testing"
)

func TestParserCreateTableSimple(t *testing.T) {
	query := `CREATE TABLE account (id INT, email TEXT)`
	parse(query, 1, t)
}

func TestParserCreateTableSimpleWithPrimaryKey(t *testing.T) {
	query := `CREATE TABLE account (id INT PRIMARY KEY, email TEXT)`
	parse(query, 1, t)
}

func TestParserMultipleInstructions(t *testing.T) {
	query := `CREATE TABLE account (id INT, email TEXT);CREATE TABLE user (id INT, email TEXT)`
	parse(query, 2, t)
}

// func TestParserLowerCase(t *testing.T) {
// 	query := `create table account (id INT PRIMARY KEY NOT NULL)`
// parse(query, 1, t)
// }

func TestParserComplete(t *testing.T) {
	query := `CREATE TABLE user
	(
    	id INT PRIMARY KEY,
	    last_name TEXT,
	    first_name TEXT,
	    email TEXT,
	    birth_date DATE,
	    country TEXT,
	    town TEXT,
	    zip_code TEXT
	)`

	parse(query, 1, t)
}

// func TestParserCreateTableWithVarchar(t *testing.T) {
// 	query := `CREATE TABLE user
// 	(
//     	id INT PRIMARY KEY,
// 	    last_name VARCHAR(100)
// 	)`
// parse(query, 1, t)
//  }

func TestSelectStar(t *testing.T) {
	query := `SELECT * FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectMultipleAttribute(t *testing.T) {
	query := `SELECT id, email FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectOneAttribute(t *testing.T) {
	query := `SELECT id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAttributeWithTable(t *testing.T) {
	query := `SELECT account.id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAttributeWithQuotedTable(t *testing.T) {
	query := `SELECT "account".id FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestSelectAllFromTable(t *testing.T) {
	query := `SELECT "account".* FROM account WHERE email = 'foo@bar.com'`
	parse(query, 1, t)
}

func TestInsertMinimal(t *testing.T) {
	query := `INSERT INTO account ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', '4')`
	parse(query, 1, t)
}

func TestInsertNumber(t *testing.T) {
	query := `INSERT INTO account ('email', 'password', 'age') VALUES ('foo@bar.com', 'tititoto', 4)`
	parse(query, 1, t)
}

// func TestInsertImplicitAttributes(t *testing.T) {
// 	query := `INSERT INTO account VALUES ('foo@bar.com', 'tititoto', 4)`
// 	parse(query, 1, t)
// }

func parse(query string, instructionNumber int, t *testing.T) []Instruction {
	t.Log("\n\n\n")

	parser := parser{}
	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string: %s", query, err)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens : %s", err)
	}

	if len(instructions) != instructionNumber {
		t.Fatalf("Should have parsed %d instructions, got %d", instructionNumber, len(instructions))
	}

	return instructions
}
