package parser

import (
	"testing"
)

func TestParserSimple(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `CREATE TABLE account`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parser tokens <%v>", decls)
	}

	if len(instructions) != 1 {
		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
	}
}

func TestParserMultipleInstructions(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `CREATE TABLE account;CREATE TABLE bidule`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parser tokens <%v>", decls)
	}

	if len(instructions) != 2 {
		t.Fatalf("Should have parsed 2 instructions, got %d", len(instructions))
	}
}

func TestParserLowerCase(t *testing.T) {
	parser := parser{}
	lexer := lexer{}
	query := `create table account`

	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex query <%s>", query)
	}

	instructions, err := parser.parse(decls)
	if err != nil {
		t.Fatalf("Cannot parser tokens <%v>", decls)
	}

	if len(instructions) != 1 {
		t.Fatalf("Should have parsed 1 instructions, got %d", len(instructions))
	}
}
