package parser

import (
	"testing"
)

func TestParserCreateTableSimple(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `CREATE TABLE account (id INT, email TEXT)`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens : %s", err)
	}

	if len(instructions) != 1 {
		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
	}
}

func TestParserCreateTableSimpleWithPrimaryKey(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `CREATE TABLE account (id INT PRIMARY KEY, email TEXT)`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens : %s", err)
	}

	if len(instructions) != 1 {
		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
	}
}

func TestParserMultipleInstructions(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `CREATE TABLE account (id INT, email TEXT);CREATE TABLE user (id INT, email TEXT)`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens : %s", err)
	}

	if len(instructions) != 2 {
		t.Fatalf("Should have parsed 2 instructions, got %d", len(instructions))
	}
}

// func TestParserLowerCase(t *testing.T) {
// 	parser := parser{}
// 	lexer := lexer{}
// 	query := `create table account (id INT PRIMARY KEY NOT NULL)`

// 	decls, err := lexer.lex([]byte(query))
// 	if err != nil {
// 		t.Fatalf("Cannot lex query <%s>", query)
// 	}

// 	instructions, err := parser.parse(decls)
// 	if err != nil {
// 		t.Fatalf("Cannot parse tokens : %s", err)
// 	}

// 	if len(instructions) != 1 {
// 		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
// 	}
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

	parser := parser{}
	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parse tokens : %s", err)
	}

	if len(instructions) != 1 {
		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
	}

	instructions[0].PrettyPrint()
	// t.Fail()
}

// func TestParserCreateTableWithVarchar(t *testing.T) {
// 	query := `CREATE TABLE user
// 	(
//     	id INT PRIMARY KEY,
// 	    last_name VARCHAR(100)
// 	)`

// 	parser := parser{}
// 	lexer := lexer{}
// 	decls, err := lexer.lex([]byte(query))
// 	if err != nil {
// 		t.Fatalf("Cannot lex <%s> string", query)
// 	}

// 	instructions, err := parser.parse(decls)
// 	if err != nil {
// 		t.Fatalf("Cannot parse tokens : %s", err)
// 	}

// 	if len(instructions) != 1 {
// 		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
// 	}
// }
