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
