package parser

import (
	"testing"
)

func TestLexerSimple(t *testing.T) {
	query := `CREATE TABLE account`

	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	if len(decls) != 5 {
		t.Fatalf("Lexing failed, expected 5 tokens, got %d", len(decls))
	}
}

// func TestLexerComplete(t *testing.T) {
// 	query := `CREATE TABLE user
// 	(
//     	id INT PRIMARY KEY NOT NULL,
// 	    last_name VARCHAR(100),
// 	    first_name VARCHAR(100),
// 	    email VARCHAR(255),
// 	    birth_date DATE,
// 	    country VARCHAR(255),
// 	    town VARCHAR(255),
// 	    zip_code VARCHAR(5)
// 	)`

// 	lexer := lexer{}
// 	_, err := lexer.lex([]byte(query))
// 	if err != nil {
// 		t.Fatalf("Cannot lex <%s> string", query)
// 	}
// }
