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
	// QuoteToken
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
	JoinToken
	OnToken
	IfToken
	NotToken
	ExistsToken
	NullToken
	AutoincrementToken

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
	matchers = append(matchers, l.MatchSimpleQuoteToken)
	matchers = append(matchers, l.MatchEqualityToken)
	matchers = append(matchers, l.MatchPeriodToken)
	matchers = append(matchers, l.MatchDoubleQuoteToken)
	// First order Matcher
	matchers = append(matchers, l.MatchCreateToken)
	matchers = append(matchers, l.MatchSelectToken)
	matchers = append(matchers, l.MatchInsertToken)
	matchers = append(matchers, l.MatchDeleteToken)
	// Second order Matcher
	matchers = append(matchers, l.MatchTableToken)
	matchers = append(matchers, l.MatchFromToken)
	matchers = append(matchers, l.MatchWhereToken)
	matchers = append(matchers, l.MatchIntoToken)
	matchers = append(matchers, l.MatchValuesToken)
	matchers = append(matchers, l.MatchJoinToken)
	matchers = append(matchers, l.MatchOnToken)
	matchers = append(matchers, l.MatchIfToken)
	matchers = append(matchers, l.MatchNotToken)
	matchers = append(matchers, l.MatchExistsToken)
	matchers = append(matchers, l.MatchNullToken)
	matchers = append(matchers, l.MatchAutoincrementToken)
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

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'C' &&
		l.instruction[l.pos+1] == 'R' &&
		l.instruction[l.pos+2] == 'E' &&
		l.instruction[l.pos+3] == 'A' &&
		l.instruction[l.pos+4] == 'T' &&
		l.instruction[l.pos+5] == 'E' {

		t := Token{
			Token:  CreateToken,
			Lexeme: "CREATE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'c' &&
		l.instruction[l.pos+1] == 'r' &&
		l.instruction[l.pos+2] == 'e' &&
		l.instruction[l.pos+3] == 'a' &&
		l.instruction[l.pos+4] == 't' &&
		l.instruction[l.pos+5] == 'e' {

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

	if l.instruction[l.pos] == 'f' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'r' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'o' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'm' {

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

	if l.pos+4 < l.instructionLen && l.instruction[l.pos] == 'T' &&
		l.instruction[l.pos+1] == 'A' &&
		l.instruction[l.pos+2] == 'B' &&
		l.instruction[l.pos+3] == 'L' &&
		l.instruction[l.pos+4] == 'E' {

		t := Token{
			Token:  TableToken,
			Lexeme: "TABLE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 5
		return true
	}

	if l.pos+4 < l.instructionLen && l.instruction[l.pos] == 't' &&
		l.instruction[l.pos+1] == 'a' &&
		l.instruction[l.pos+2] == 'b' &&
		l.instruction[l.pos+3] == 'l' &&
		l.instruction[l.pos+4] == 'e' {

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

func (l *lexer) MatchNullToken() bool {

	if l.pos+3 < l.instructionLen && l.instruction[l.pos] == 'N' &&
		l.instruction[l.pos+1] == 'U' &&
		l.instruction[l.pos+2] == 'L' &&
		l.instruction[l.pos+3] == 'L' {

		t := Token{
			Token:  NullToken,
			Lexeme: "NULL",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+3 < l.instructionLen && l.instruction[l.pos] == 'n' &&
		l.instruction[l.pos+1] == 'u' &&
		l.instruction[l.pos+2] == 'l' &&
		l.instruction[l.pos+3] == 'l' {

		t := Token{
			Token:  NullToken,
			Lexeme: "NULL",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchIfToken() bool {

	if l.pos+1 < l.instructionLen && l.instruction[l.pos] == 'I' &&
		l.instruction[l.pos+1] == 'F' {

		t := Token{
			Token:  IfToken,
			Lexeme: "IF",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+1 < l.instructionLen && l.instruction[l.pos] == 'i' &&
		l.instruction[l.pos+1] == 'f' {

		t := Token{
			Token:  IfToken,
			Lexeme: "IF",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchNotToken() bool {

	if l.pos+2 < l.instructionLen && l.instruction[l.pos] == 'N' &&
		l.instruction[l.pos+1] == 'O' &&
		l.instruction[l.pos+2] == 'T' {

		t := Token{
			Token:  NotToken,
			Lexeme: "NOT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+2 < l.instructionLen && l.instruction[l.pos] == 'n' &&
		l.instruction[l.pos+1] == 'o' &&
		l.instruction[l.pos+2] == 't' {

		t := Token{
			Token:  NotToken,
			Lexeme: "NOT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchExistsToken() bool {

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'E' &&
		l.instruction[l.pos+1] == 'X' &&
		l.instruction[l.pos+2] == 'I' &&
		l.instruction[l.pos+3] == 'S' &&
		l.instruction[l.pos+4] == 'T' &&
		l.instruction[l.pos+5] == 'S' {

		t := Token{
			Token:  ExistsToken,
			Lexeme: "EXISTS",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'e' &&
		l.instruction[l.pos+1] == 'x' &&
		l.instruction[l.pos+2] == 'i' &&
		l.instruction[l.pos+3] == 's' &&
		l.instruction[l.pos+4] == 't' &&
		l.instruction[l.pos+5] == 's' {

		t := Token{
			Token:  ExistsToken,
			Lexeme: "EXISTS",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchDeleteToken() bool {

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'D' &&
		l.instruction[l.pos+1] == 'E' &&
		l.instruction[l.pos+2] == 'L' &&
		l.instruction[l.pos+3] == 'E' &&
		l.instruction[l.pos+4] == 'T' &&
		l.instruction[l.pos+5] == 'E' {

		t := Token{
			Token:  DeleteToken,
			Lexeme: "DELETE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+5 < l.instructionLen && l.instruction[l.pos] == 'd' &&
		l.instruction[l.pos+1] == 'e' &&
		l.instruction[l.pos+2] == 'l' &&
		l.instruction[l.pos+3] == 'e' &&
		l.instruction[l.pos+4] == 't' &&
		l.instruction[l.pos+5] == 'e' {

		t := Token{
			Token:  DeleteToken,
			Lexeme: "DELETE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchAutoincrementToken() bool {

	if l.pos+12 < l.instructionLen && l.instruction[l.pos] == 'A' &&
		l.instruction[l.pos+1] == 'U' &&
		l.instruction[l.pos+2] == 'T' &&
		l.instruction[l.pos+3] == 'O' &&
		l.instruction[l.pos+4] == 'I' &&
		l.instruction[l.pos+5] == 'N' &&
		l.instruction[l.pos+6] == 'C' &&
		l.instruction[l.pos+7] == 'R' &&
		l.instruction[l.pos+8] == 'E' &&
		l.instruction[l.pos+9] == 'M' &&
		l.instruction[l.pos+10] == 'E' &&
		l.instruction[l.pos+11] == 'N' &&
		l.instruction[l.pos+12] == 'T' {

		t := Token{
			Token:  AutoincrementToken,
			Lexeme: "AUTOINCREMENT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+12 < l.instructionLen && l.instruction[l.pos] == 'a' &&
		l.instruction[l.pos+1] == 'u' &&
		l.instruction[l.pos+2] == 't' &&
		l.instruction[l.pos+3] == 'o' &&
		l.instruction[l.pos+4] == 'i' &&
		l.instruction[l.pos+5] == 'n' &&
		l.instruction[l.pos+6] == 'c' &&
		l.instruction[l.pos+7] == 'r' &&
		l.instruction[l.pos+8] == 'e' &&
		l.instruction[l.pos+9] == 'm' &&
		l.instruction[l.pos+10] == 'e' &&
		l.instruction[l.pos+11] == 'n' &&
		l.instruction[l.pos+12] == 't' {

		t := Token{
			Token:  AutoincrementToken,
			Lexeme: "AUTOINCREMENT",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchPrimaryToken() bool {

	if l.pos+6 < l.instructionLen && l.instruction[l.pos] == 'P' &&
		l.instruction[l.pos+1] == 'R' &&
		l.instruction[l.pos+2] == 'I' &&
		l.instruction[l.pos+3] == 'M' &&
		l.instruction[l.pos+4] == 'A' &&
		l.instruction[l.pos+5] == 'R' &&
		l.instruction[l.pos+6] == 'Y' {

		t := Token{
			Token:  PrimaryToken,
			Lexeme: "PRIMARY",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	if l.pos+6 < l.instructionLen && l.instruction[l.pos] == 'p' &&
		l.instruction[l.pos+1] == 'r' &&
		l.instruction[l.pos+2] == 'i' &&
		l.instruction[l.pos+3] == 'm' &&
		l.instruction[l.pos+4] == 'a' &&
		l.instruction[l.pos+5] == 'r' &&
		l.instruction[l.pos+6] == 'y' {

		t := Token{
			Token:  PrimaryToken,
			Lexeme: "PRIMARY",
		}
		l.tokens = append(l.tokens, t)
		l.pos += len(t.Lexeme)
		return true
	}

	return false
}

func (l *lexer) MatchKeyToken() bool {

	if l.pos+2 < l.instructionLen && l.instruction[l.pos] == 'K' &&
		l.instruction[l.pos+1] == 'E' &&
		l.instruction[l.pos+2] == 'Y' {

		t := Token{
			Token:  KeyToken,
			Lexeme: "KEY",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 3
		return true
	}

	if l.pos+2 < l.instructionLen && l.instruction[l.pos] == 'k' &&
		l.instruction[l.pos+1] == 'e' &&
		l.instruction[l.pos+2] == 'y' {

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

func (l *lexer) MatchJoinToken() bool {

	if l.instruction[l.pos] == 'J' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'O' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'I' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'N' {

		t := Token{
			Token:  JoinToken,
			Lexeme: "JOIN",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 4
		return true
	}

	return false
}

func (l *lexer) MatchOnToken() bool {

	if l.instruction[l.pos] == 'O' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'N' {

		t := Token{
			Token:  OnToken,
			Lexeme: "ON",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 2
		return true
	}

	return false
}

func (l *lexer) MatchStringToken() bool {

	i := l.pos
	for i < l.instructionLen &&
		(unicode.IsLetter(rune(l.instruction[i])) || l.instruction[i] == '_' ||
			l.instruction[i] == '@' /* || l.instruction[i] == '.'*/) {
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

// func (l *lexer) MatchQuoteToken() bool {

// 	if l.instruction[l.pos] == '\'' {
// 		t := Token{
// 			Token:  QuoteToken,
// 			Lexeme: "'",
// 		}
// 		l.tokens = append(l.tokens, t)
// 		l.pos += 1
// 		return true
// 	}

// 	return false
// }

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

		if l.MatchSingleQuotedStringToken() {
			t := Token{
				Token:  SimpleQuoteToken,
				Lexeme: "'",
			}
			l.tokens = append(l.tokens, t)
			l.pos += 1
			return true
		}

		return true
	}

	return false
}

func (l *lexer) MatchSingleQuotedStringToken() bool {
	i := l.pos
	for i < l.instructionLen && l.instruction[i] != '\'' {
		i++
	}
	if i == l.pos {
		return false
	}

	t := Token{
		Token:  StringToken,
		Lexeme: string(l.instruction[l.pos:i]),
	}
	l.tokens = append(l.tokens, t)
	l.pos = i

	return true
}
