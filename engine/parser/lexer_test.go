package parser

import (
	"testing"
	"time"
)

func TestLexerSimple(t *testing.T) {
	query := `CREATE TABLE `+"`"+`account`+"`"+``

	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	if len(decls) != 7 {
		t.Fatalf("Lexing failed, expected 7 tokens, got %d", len(decls))
	}
}

func TestParseDate(t *testing.T) {
	const long = "2006-01-02 15:04:05.999999999 -0700 MST"
	data := `2015-09-10 14:03:09.444695269 +0200 CEST`

	_, err := time.Parse(long, data)
	if err != nil {
		t.Fatalf("Cannot parse %s: %s", data, err)
	}
}

func TestLexerWithGTOEandLTOEOperator(t *testing.T) {
	query := `SELECT FROM foo WHERE 1 >= 1 AND 2 <= 3`

	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	if len(decls) != 21 {
		t.Fatalf("Lexing failed, expected 21 tokens, got %d", len(decls))
	}
}
