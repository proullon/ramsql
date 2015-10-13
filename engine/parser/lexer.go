package parser

import (
	"fmt"
	"unicode"

	"github.com/proullon/ramsql/engine/log"
)

// SQL Tokens
const (
	// Ponctuation token

	SpaceToken = iota
	SemicolonToken
	CommaToken
	BracketOpeningToken
	BracketClosingToken
	LeftDipleToken
	RightDipleToken

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
	TruncateToken

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
	CountToken
	SetToken
	OrderToken
	ByToken
	WithToken
	TimeToken
	ZoneToken
	ReturningToken
	InToken
	AndToken
	OrToken

	// Type Token

	TextToken
	IntToken
	PrimaryToken
	KeyToken
	StringToken
	NumberToken
	DateToken
)

// Token struct holds token id and it's lexeme
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

// Matcher tries to match given string to an SQL token
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
	matchers = append(matchers, l.MatchLeftDipleToken)
	matchers = append(matchers, l.MatchRightDipleToken)
	// First order Matcher
	matchers = append(matchers, l.MatchCreateToken)
	matchers = append(matchers, l.MatchSelectToken)
	matchers = append(matchers, l.MatchInsertToken)
	matchers = append(matchers, l.MatchUpdateToken)
	matchers = append(matchers, l.MatchDeleteToken)
	matchers = append(matchers, l.MatchTruncateToken)
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
	matchers = append(matchers, l.MatchCountToken)
	matchers = append(matchers, l.MatchSetToken)
	matchers = append(matchers, l.MatchOrderToken)
	matchers = append(matchers, l.MatchByToken)
	matchers = append(matchers, l.MatchWithToken)
	matchers = append(matchers, l.MatchTimeToken)
	matchers = append(matchers, l.MatchZoneToken)
	matchers = append(matchers, l.MatchReturningToken)
	matchers = append(matchers, l.MatchInToken)
	matchers = append(matchers, l.MatchAndToken)
	matchers = append(matchers, l.MatchOrToken)
	// Type Matcher
	matchers = append(matchers, l.MatchPrimaryToken)
	matchers = append(matchers, l.MatchKeyToken)
	matchers = append(matchers, l.MatchDateToken)
	matchers = append(matchers, l.MatchNumberToken)
	matchers = append(matchers, l.MatchStringToken)
	matchers = append(matchers, l.MatchEscapedStringToken)

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
			log.Warning("Cannor lex <%s>, stuck at pos %d -> [%c]", l.instruction, l.pos, l.instruction[l.pos])
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
		l.pos++
		return true
	}

	return false
}

func (l *lexer) MatchAndToken() bool {
	return l.Match([]byte("and"), AndToken)
}

func (l *lexer) MatchOrToken() bool {
	return l.Match([]byte("or"), OrToken)
}

func (l *lexer) MatchInToken() bool {
	return l.Match([]byte("in"), InToken)
}

func (l *lexer) MatchReturningToken() bool {
	return l.Match([]byte("returning"), ReturningToken)
}

func (l *lexer) MatchTruncateToken() bool {
	return l.Match([]byte("truncate"), TruncateToken)
}

func (l *lexer) MatchWithToken() bool {
	return l.Match([]byte("with"), WithToken)
}

func (l *lexer) MatchTimeToken() bool {
	return l.Match([]byte("time"), TimeToken)
}

func (l *lexer) MatchZoneToken() bool {
	return l.Match([]byte("zone"), ZoneToken)
}

func (l *lexer) MatchOrderToken() bool {
	return l.Match([]byte("order"), OrderToken)
}

func (l *lexer) MatchByToken() bool {
	return l.Match([]byte("by"), ByToken)
}

func (l *lexer) MatchSetToken() bool {
	return l.Match([]byte("set"), SetToken)
}

func (l *lexer) MatchUpdateToken() bool {
	return l.Match([]byte("update"), UpdateToken)
}

func (l *lexer) MatchCreateToken() bool {
	return l.Match([]byte("create"), CreateToken)
}

func (l *lexer) MatchSelectToken() bool {
	return l.Match([]byte("select"), SelectToken)
}

func (l *lexer) MatchInsertToken() bool {
	return l.Match([]byte("insert"), InsertToken)
}

func (l *lexer) MatchWhereToken() bool {
	return l.Match([]byte("where"), WhereToken)
}

func (l *lexer) MatchFromToken() bool {
	return l.Match([]byte("from"), FromToken)
}

func (l *lexer) MatchTableToken() bool {
	return l.Match([]byte("table"), TableToken)
}

func (l *lexer) MatchNullToken() bool {
	return l.Match([]byte("null"), NullToken)
}

func (l *lexer) MatchIfToken() bool {
	return l.Match([]byte("if"), IfToken)
}

func (l *lexer) MatchNotToken() bool {
	return l.Match([]byte("not"), NotToken)
}

func (l *lexer) MatchExistsToken() bool {
	return l.Match([]byte("exists"), ExistsToken)
}

func (l *lexer) MatchCountToken() bool {
	return l.Match([]byte("count"), CountToken)
}

func (l *lexer) MatchDeleteToken() bool {
	return l.Match([]byte("delete"), DeleteToken)
}

func (l *lexer) MatchAutoincrementToken() bool {
	return l.Match([]byte("autoincrement"), AutoincrementToken)
}

func (l *lexer) MatchPrimaryToken() bool {
	return l.Match([]byte("primary"), PrimaryToken)
}

func (l *lexer) MatchKeyToken() bool {
	return l.Match([]byte("key"), KeyToken)
}

func (l *lexer) MatchIntoToken() bool {
	return l.Match([]byte("into"), IntoToken)
}

func (l *lexer) MatchValuesToken() bool {
	return l.Match([]byte("values"), ValuesToken)
}

func (l *lexer) MatchJoinToken() bool {
	return l.Match([]byte("join"), JoinToken)
}

func (l *lexer) MatchOnToken() bool {
	return l.Match([]byte("on"), OnToken)
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
	return l.MatchSingle(';', SemicolonToken)
}

func (l *lexer) MatchPeriodToken() bool {
	return l.MatchSingle('.', PeriodToken)
}

func (l *lexer) MatchBracketOpeningToken() bool {
	return l.MatchSingle('(', BracketOpeningToken)
}

func (l *lexer) MatchBracketClosingToken() bool {
	return l.MatchSingle(')', BracketClosingToken)
}

func (l *lexer) MatchCommaToken() bool {
	return l.MatchSingle(',', CommaToken)
}

func (l *lexer) MatchStarToken() bool {
	return l.MatchSingle('*', StarToken)
}

func (l *lexer) MatchEqualityToken() bool {
	return l.MatchSingle('=', EqualityToken)
}

func (l *lexer) MatchLeftDipleToken() bool {
	return l.MatchSingle('<', LeftDipleToken)
}

func (l *lexer) MatchRightDipleToken() bool {
	return l.MatchSingle('>', RightDipleToken)
}

// 2015-09-10 14:03:09.444695269 +0200 CEST);
func (l *lexer) MatchDateToken() bool {

	i := l.pos
	for i < l.instructionLen &&
		l.instruction[i] != ',' &&
		l.instruction[i] != ')' {
		i++
	}

	data := string(l.instruction[l.pos:i])

	_, err := ParseDate(data)
	if err != nil {
		return false
	}

	t := Token{
		Token:  StringToken,
		Lexeme: data,
	}

	l.tokens = append(l.tokens, t)
	l.pos = i
	return true
}

func (l *lexer) MatchDoubleQuoteToken() bool {

	if l.instruction[l.pos] == '"' {

		t := Token{
			Token:  DoubleQuoteToken,
			Lexeme: "\"",
		}
		l.tokens = append(l.tokens, t)
		l.pos++

		if l.MatchDoubleQuotedStringToken() {
			t := Token{
				Token:  DoubleQuoteToken,
				Lexeme: "\"",
			}
			l.tokens = append(l.tokens, t)
			l.pos++
			return true
		}

		return true
	}

	return false
}

func (l *lexer) MatchEscapedStringToken() bool {
	i := l.pos
	if l.instruction[i] != '$' || l.instruction[i+1] != '$' {
		return false
	}
	i += 2

	for i+1 < l.instructionLen && l.instruction[i] != '$' && l.instruction[i+1] != '$' {
		i++
	}
	i++

	if i+1 == l.instructionLen {
		return false
	}

	tok := NumberToken
	escaped := l.instruction[l.pos+2 : i]

	for _, r := range escaped {
		if unicode.IsDigit(rune(r)) == false {
			tok = StringToken
		}
	}

	_, err := ParseDate(string(escaped))
	if err == nil {
		tok = DateToken
	}

	t := Token{
		Token:  tok,
		Lexeme: string(escaped),
	}
	l.tokens = append(l.tokens, t)
	l.pos = i + 2

	return true
}

func (l *lexer) MatchDoubleQuotedStringToken() bool {
	i := l.pos
	for i < l.instructionLen && l.instruction[i] != '"' {
		i++
	}

	t := Token{
		Token:  StringToken,
		Lexeme: string(l.instruction[l.pos:i]),
	}
	l.tokens = append(l.tokens, t)
	l.pos = i

	return true
}

func (l *lexer) MatchSimpleQuoteToken() bool {

	if l.instruction[l.pos] == '\'' {

		t := Token{
			Token:  SimpleQuoteToken,
			Lexeme: "'",
		}
		l.tokens = append(l.tokens, t)
		l.pos++

		if l.MatchSingleQuotedStringToken() {
			t := Token{
				Token:  SimpleQuoteToken,
				Lexeme: "'",
			}
			l.tokens = append(l.tokens, t)
			l.pos++
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

	t := Token{
		Token:  StringToken,
		Lexeme: string(l.instruction[l.pos:i]),
	}
	l.tokens = append(l.tokens, t)
	l.pos = i

	return true
}

func (l *lexer) MatchSingle(char byte, token int) bool {

	if l.pos > l.instructionLen {
		return false
	}

	if l.instruction[l.pos] != char {
		return false
	}

	t := Token{
		Token:  token,
		Lexeme: string(char),
	}

	l.tokens = append(l.tokens, t)
	l.pos++
	return true
}

func (l *lexer) Match(str []byte, token int) bool {

	if l.pos+len(str)-1 > l.instructionLen {
		return false
	}

	// Check for lowercase and uppercase
	for i := range str {
		if l.instruction[l.pos+i] != str[i] &&
			l.instruction[l.pos+i] != byte(str[i]-32) {
			return false
		}
	}

	// if next character is still a string, it means it doesn't match
	// ie: COUNT shoulnd match COUNTRY
	if unicode.IsLetter(rune(l.instruction[l.pos+len(str)])) ||
		l.instruction[l.pos+len(str)] == '_' {
		return false
	}

	t := Token{
		Token:  token,
		Lexeme: string(str),
	}

	l.tokens = append(l.tokens, t)
	l.pos += len(t.Lexeme)
	return true
}
