package parser

import (
	"testing"
	"time"
)

func TestLexerSimple(t *testing.T) {
	query := `CREATE TABLE ` + "`" + `account` + "`" + ``

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

func TestLexerWithNEOperator(t *testing.T) {
	query := `SELECT FROM foo WHERE 0 <> 1 AND 2 <> 3`

	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	if len(decls) != 21 {
		t.Fatalf("Lexing failed, expected 21 tokens, got %d", len(decls))
	}
}

func TestLexerWithInsertScientificNotation(t *testing.T) {
	query := `INSERT INTO foo (substance, mass) values ('MnO2', 8694e-2)`

	lexer := lexer{}
	decls, err := lexer.lex([]byte(query))
	if err != nil {
		t.Fatalf("Cannot lex <%s> string", query)
	}

	if len(decls) != 23 {
		t.Fatalf("Lexing failed, expected 21 tokens, got %d", len(decls))
	}
}

func Test_lexer_MatchNumberToken(t *testing.T) {
	tests := []struct {
		name string
		l    *lexer
		want bool
	}{
		{
			name: "should pass; thirty thousand",
			l: &lexer{
				instruction:    []byte("300000"),
				instructionLen: len("300000"),
			},
			want: true,
		},
		{
			name: "should pass; floating point",
			l: &lexer{
				instruction:    []byte("3.000000"),
				instructionLen: len("3.000000"),
			},
			want: true,
		},
		{
			name: "should pass; scientific notation",
			l: &lexer{
				instruction:    []byte("3e+6"),
				instructionLen: len("3e+6"),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.MatchNumberToken(); got != tt.want {
				t.Errorf("lexer.MatchNumberToken() = %v, want %v", got, tt.want)
			}
		})
	}
}
