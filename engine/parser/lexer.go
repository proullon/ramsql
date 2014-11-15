package parser

import (
	"fmt"
	"log"
	"unicode"
)

const (
	// Ponctuation token
	SpaceToken = iota
	SemicolonToken
	CommaToken
	BracketOpeningToken
	BracketClosingToken
	QuoteToken
	DoubleQuoteToken
	SimpleQuoteToken
	StarToken
	EqualityToken
	PeriodToken

	// First order Token
	CreateToken
	SelectToken
	InsertToken
	UpdateToken
	DeleteToken
	ExplainToken

	// Second order Token
	FromToken
	WhereToken
	TableToken
	IntoToken
	ValuesToken

	// Type Token
	TextToken
	IntToken
	PrimaryToken
	KeyToken
	StringToken
	NumberToken
)

type Token struct {
	Token  int
	Lexeme string
}

type lexer struct {
	tokens         []Token
	instruction    []byte
	instructionLen int
	pos            int
}

type Matcher func() bool

func (l *lexer) lex(instruction []byte) ([]Token, error) {
	// log.Printf("lexer.lex : <%s>", instruction)
	l.instructionLen = len(instruction)
	l.tokens = nil
	l.instruction = instruction
	l.pos = 0
	securityPos := 0

	var matchers []Matcher
	// Ponctuation Matcher
	matchers = append(matchers, l.MatchSpaceToken)
	matchers = append(matchers, l.MatchSemicolonToken)
	matchers = append(matchers, l.MatchCommaToken)
	matchers = append(matchers, l.MatchBracketOpeningToken)
	matchers = append(matchers, l.MatchBracketClosingToken)
	matchers = append(matchers, l.MatchStarToken)
	matchers = append(matchers, l.MatchQuoteToken)
	matchers = append(matchers, l.MatchEqualityToken)
	matchers = append(matchers, l.MatchSimpleQuoteToken)
	matchers = append(matchers, l.MatchPeriodToken)
	matchers = append(matchers, l.MatchDoubleQuoteToken)
	// First order Matcher
	matchers = append(matchers, l.MatchCreateToken)
	matchers = append(matchers, l.MatchSelectToken)
	matchers = append(matchers, l.MatchInsertToken)
	// Second order Matcher
	matchers = append(matchers, l.MatchTableToken)
	matchers = append(matchers, l.MatchFromToken)
	matchers = append(matchers, l.MatchWhereToken)
	matchers = append(matchers, l.MatchIntoToken)
	matchers = append(matchers, l.MatchValuesToken)
	// Type Matcher
	matchers = append(matchers, l.MatchPrimaryToken)
	matchers = append(matchers, l.MatchKeyToken)
	matchers = append(matchers, l.MatchNumberToken)
	matchers = append(matchers, l.MatchStringToken)

	var r bool
	for l.pos < l.instructionLen {
		// fmt.Printf("Tokens : %v\n\n", l.tokens)

		r = false
		for _, m := range matchers {
			if r = m(); r == true {
				securityPos = l.pos
				break
			}
		}

		if r {
			continue
		}

		if l.pos == securityPos {
			log.Printf("Cannot lex <%s>, stuck at pos %d -> [%c]", l.instruction, l.pos, l.instruction[l.pos])
			return nil, fmt.Errorf("Cannot lex instruction. Syntax error near %s", instruction[l.pos:])
		}
		securityPos = l.pos
	}

	return l.tokens, nil
}

func (l *lexer) MatchSpaceToken() bool {

	if unicode.IsSpace(rune(l.instruction[l.pos])) {
		t := Token{
			Token:  SpaceToken,
			Lexeme: " ",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchCreateToken() bool {

	if l.instruction[l.pos] == 'C' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'R' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'E' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'A' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'T' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'E' {

		t := Token{
			Token:  CreateToken,
			Lexeme: "CREATE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	return false
}

func (l *lexer) MatchSelectToken() bool {

	if l.instruction[l.pos] == 'S' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'E' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'L' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'E' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'C' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'T' {

		t := Token{
			Token:  SelectToken,
			Lexeme: "SELECT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	return false
}

func (l *lexer) MatchInsertToken() bool {

	if l.instruction[l.pos] == 'I' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'N' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'S' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'E' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'R' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'T' {

		t := Token{
			Token:  InsertToken,
			Lexeme: "INSERT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	return false
}

func (l *lexer) MatchWhereToken() bool {

	if l.instruction[l.pos] == 'W' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'H' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'E' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'R' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'E' {

		t := Token{
			Token:  WhereToken,
			Lexeme: "WHERE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 5
		return true
	}

	return false
}

func (l *lexer) MatchFromToken() bool {

	if l.instruction[l.pos] == 'F' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'R' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'O' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'M' {

		t := Token{
			Token:  FromToken,
			Lexeme: "FROM",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 4
		return true
	}

	return false
}

func (l *lexer) MatchTableToken() bool {

	if l.instruction[l.pos] == 'T' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'A' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'B' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'L' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'E' {

		t := Token{
			Token:  TableToken,
			Lexeme: "TABLE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 5
		return true
	}

	return false
}

func (l *lexer) MatchPrimaryToken() bool {

	if l.instruction[l.pos] == 'P' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'R' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'I' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'M' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'A' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'R' &&
		l.pos+6 < l.instructionLen && l.instruction[l.pos+6] == 'Y' {

		t := Token{
			Token:  PrimaryToken,
			Lexeme: "PRIMARY",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 7
		return true
	}

	return false
}

func (l *lexer) MatchKeyToken() bool {

	if l.instruction[l.pos] == 'K' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'E' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'Y' {

		t := Token{
			Token:  KeyToken,
			Lexeme: "KEY",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 3
		return true
	}

	return false
}

func (l *lexer) MatchIntoToken() bool {

	if l.instruction[l.pos] == 'I' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'N' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'T' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'O' {

		t := Token{
			Token:  IntoToken,
			Lexeme: "INTO",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 4
		return true
	}

	return false
}

func (l *lexer) MatchValuesToken() bool {

	if l.instruction[l.pos] == 'V' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'A' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'L' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'U' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'E' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'S' {

		t := Token{
			Token:  ValuesToken,
			Lexeme: "VALUES",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	return false
}

func (l *lexer) MatchStringToken() bool {

	i := l.pos
	for i < l.instructionLen &&
		(unicode.IsLetter(rune(l.instruction[i])) || l.instruction[i] == '_' ||
			l.instruction[i] == '@' || l.instruction[i] == '.') {
		i++
	}

	if i != l.pos {
		t := Token{
			Token:  StringToken,
			Lexeme: string(l.instruction[l.pos:i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchNumberToken() bool {

	i := l.pos
	for i < l.instructionLen && unicode.IsDigit(rune(l.instruction[i])) {
		i++
	}

	if i != l.pos {
		t := Token{
			Token:  NumberToken,
			Lexeme: string(l.instruction[l.pos:i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchSemicolonToken() bool {

	if l.instruction[l.pos] == ';' {
		t := Token{
			Token:  SemicolonToken,
			Lexeme: ";",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchDoubleQuoteToken() bool {

	if l.instruction[l.pos] == '"' {
		t := Token{
			Token:  DoubleQuoteToken,
			Lexeme: "\"",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchPeriodToken() bool {

	if l.instruction[l.pos] == '.' {
		t := Token{
			Token:  PeriodToken,
			Lexeme: ".",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchBracketOpeningToken() bool {

	if l.instruction[l.pos] == '(' {
		t := Token{
			Token:  BracketOpeningToken,
			Lexeme: "(",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchBracketClosingToken() bool {

	if l.instruction[l.pos] == ')' {
		t := Token{
			Token:  BracketClosingToken,
			Lexeme: ")",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchCommaToken() bool {

	if l.instruction[l.pos] == ',' {
		t := Token{
			Token:  CommaToken,
			Lexeme: ",",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchStarToken() bool {

	if l.instruction[l.pos] == '*' {
		t := Token{
			Token:  StarToken,
			Lexeme: "*",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchQuoteToken() bool {

	if l.instruction[l.pos] == '\'' {
		t := Token{
			Token:  QuoteToken,
			Lexeme: "'",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchEqualityToken() bool {

	if l.instruction[l.pos] == '=' {
		t := Token{
			Token:  EqualityToken,
			Lexeme: "=",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchSimpleQuoteToken() bool {

	if l.instruction[l.pos] == '\'' {
		t := Token{
			Token:  SimpleQuoteToken,
			Lexeme: "'",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}
