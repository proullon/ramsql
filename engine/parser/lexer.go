package parser

import (
	"fmt"
	"unicode"

	"github.com/proullon/ramsql/engine/log"
)

// SQL Tokens
const (
	// Punctuation token

	SpaceToken = iota
	SemicolonToken
	CommaToken
	BracketOpeningToken
	BracketClosingToken
	LeftDipleToken
	RightDipleToken
	LessOrEqualToken
	GreaterOrEqualToken
	BacktickToken

	// QuoteToken

	DoubleQuoteToken
	SimpleQuoteToken
	StarToken
	EqualityToken
	DistinctnessToken
	PeriodToken

	// First order Token

	CreateToken
	SelectToken
	InsertToken
	UpdateToken
	DeleteToken
	ExplainToken
	TruncateToken
	DropToken
	GrantToken
	DistinctToken

	// Second order Token

	FromToken
	WhereToken
	TableToken
	SchemaToken
	CurrentSchemaToken
	IntoToken
	ValuesToken
	JoinToken
	AsToken
	OnToken
	IfToken
	NotToken
	ExistsToken
	NullToken
	UnsignedToken
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
	AscToken
	DescToken
	LimitToken
	IsToken
	ForToken
	DefaultToken
	LocalTimestampToken
	FalseToken
	UniqueToken
	NowToken
	OffsetToken
	IndexToken
	CollateToken
	NocaseToken

	// Type Token

	TextToken
	IntToken
	PrimaryToken
	KeyToken
	StringToken
	DecimalToken
	NumberToken
	DateToken
	FloatToken

	ArgToken
	NamedArgToken
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
	matchers = append(matchers, l.MatchArgTokenODBC)
	matchers = append(matchers, l.MatchNamedArgToken)
	matchers = append(matchers, l.MatchArgToken)
	matchers = append(matchers, l.MatchFloatToken)
	// Punctuation Matcher
	matchers = append(matchers, l.MatchSpaceToken)
	matchers = append(matchers, l.genericByteMatcher(';', SemicolonToken))
	matchers = append(matchers, l.genericByteMatcher(',', CommaToken))
	matchers = append(matchers, l.genericByteMatcher('(', BracketOpeningToken))
	matchers = append(matchers, l.genericByteMatcher(')', BracketClosingToken))
	matchers = append(matchers, l.genericByteMatcher('*', StarToken))
	matchers = append(matchers, l.MatchSimpleQuoteToken)
	matchers = append(matchers, l.genericByteMatcher('=', EqualityToken))
	matchers = append(matchers, l.genericStringMatcher("<>", DistinctnessToken))
	matchers = append(matchers, l.genericStringMatcher("!=", DistinctnessToken))
	matchers = append(matchers, l.genericByteMatcher('.', PeriodToken))
	matchers = append(matchers, l.MatchDoubleQuoteToken)
	matchers = append(matchers, l.genericStringMatcher("<=", LessOrEqualToken))
	matchers = append(matchers, l.genericStringMatcher(">=", GreaterOrEqualToken))
	matchers = append(matchers, l.genericByteMatcher('<', LeftDipleToken))
	matchers = append(matchers, l.genericByteMatcher('>', RightDipleToken))
	matchers = append(matchers, l.genericByteMatcher('`', BacktickToken))
	// First order Matcher
	matchers = append(matchers, l.genericStringMatcher("create", CreateToken))
	matchers = append(matchers, l.genericStringMatcher("select", SelectToken))
	matchers = append(matchers, l.genericStringMatcher("insert", InsertToken))
	matchers = append(matchers, l.genericStringMatcher("update", UpdateToken))
	matchers = append(matchers, l.genericStringMatcher("delete", DeleteToken))
	matchers = append(matchers, l.genericStringMatcher("truncate", TruncateToken))
	matchers = append(matchers, l.genericStringMatcher("drop", DropToken))
	matchers = append(matchers, l.genericStringMatcher("grant", GrantToken))
	matchers = append(matchers, l.genericStringMatcher("distinct", DistinctToken))
	// Second order Matcher
	matchers = append(matchers, l.genericStringMatcher("table", TableToken))
	matchers = append(matchers, l.genericStringMatcher("current_schema()", CurrentSchemaToken))
	matchers = append(matchers, l.genericStringMatcher("current_schema", CurrentSchemaToken))
	matchers = append(matchers, l.genericStringMatcher("schema", SchemaToken))
	matchers = append(matchers, l.genericStringMatcher("from", FromToken))
	matchers = append(matchers, l.genericStringMatcher("where", WhereToken))
	matchers = append(matchers, l.genericStringMatcher("into", IntoToken))
	matchers = append(matchers, l.genericStringMatcher("values", ValuesToken))
	matchers = append(matchers, l.genericStringMatcher("join", JoinToken))
	matchers = append(matchers, l.genericStringMatcher("as", AsToken))
	matchers = append(matchers, l.genericStringMatcher("on", OnToken))
	matchers = append(matchers, l.genericStringMatcher("if", IfToken))
	matchers = append(matchers, l.genericStringMatcher("not", NotToken))
	matchers = append(matchers, l.genericStringMatcher("exists", ExistsToken))
	matchers = append(matchers, l.genericStringMatcher("null", NullToken))
	matchers = append(matchers, l.MatchAutoincrementToken)
	matchers = append(matchers, l.genericStringMatcher("unsigned", UnsignedToken))
	matchers = append(matchers, l.genericStringMatcher("count", CountToken))
	matchers = append(matchers, l.genericStringMatcher("set", SetToken))
	matchers = append(matchers, l.genericStringMatcher("order", OrderToken))
	matchers = append(matchers, l.genericStringMatcher("by", ByToken))
	matchers = append(matchers, l.genericStringMatcher("with", WithToken))
	matchers = append(matchers, l.genericStringMatcher("time", TimeToken))
	matchers = append(matchers, l.genericStringMatcher("zone", ZoneToken))
	matchers = append(matchers, l.genericStringMatcher("returning", ReturningToken))
	matchers = append(matchers, l.genericStringMatcher("in", InToken))
	matchers = append(matchers, l.genericStringMatcher("and", AndToken))
	matchers = append(matchers, l.genericStringMatcher("or", OrToken))
	matchers = append(matchers, l.genericStringMatcher("asc", AscToken))
	matchers = append(matchers, l.genericStringMatcher("desc", DescToken))
	matchers = append(matchers, l.genericStringMatcher("limit", LimitToken))
	matchers = append(matchers, l.genericStringMatcher("is", IsToken))
	matchers = append(matchers, l.genericStringMatcher("for", ForToken))
	matchers = append(matchers, l.genericStringMatcher("default", DefaultToken))
	matchers = append(matchers, l.genericStringMatcher("localtimestamp", LocalTimestampToken))
	matchers = append(matchers, l.genericStringMatcher("false", FalseToken))
	matchers = append(matchers, l.genericStringMatcher("unique", UniqueToken))
	matchers = append(matchers, l.genericStringMatcher("now()", NowToken))
	matchers = append(matchers, l.genericStringMatcher("offset", OffsetToken))
	matchers = append(matchers, l.genericStringMatcher("index", IndexToken))
	matchers = append(matchers, l.genericStringMatcher("on", OnToken))
	matchers = append(matchers, l.genericStringMatcher("collate", CollateToken))
	matchers = append(matchers, l.genericStringMatcher("nocase", NocaseToken))
	// Type Matcher
	matchers = append(matchers, l.genericStringMatcher("decimal", DecimalToken))
	matchers = append(matchers, l.genericStringMatcher("primary", PrimaryToken))
	matchers = append(matchers, l.genericStringMatcher("key", KeyToken))
	matchers = append(matchers, l.MatchEscapedStringToken)
	matchers = append(matchers, l.MatchDateToken)
	matchers = append(matchers, l.MatchNumberToken)
	matchers = append(matchers, l.MatchStringToken)

	var r bool
	for l.pos < l.instructionLen {
		r = false
		for _, m := range matchers {
			if r = m(); r {
				securityPos = l.pos
				break
			}
		}

		if r {
			continue
		}

		if l.pos == securityPos {
			log.Warn("Cannot lex <%s>, stuck at pos %d -> [%c]", l.instruction, l.pos, l.instruction[l.pos])
			return nil, fmt.Errorf("Cannot lex instruction. Syntax error near %s", instruction[l.pos:])
		}
		securityPos = l.pos
	}

	return l.tokens, nil
}

func (l *lexer) MatchArgTokenODBC() bool {

	i := l.pos
	if l.instruction[i] != '?' {
		return false
	}
	if len(l.tokens) < 1 {
		return false
	}
	if l.tokens[len(l.tokens)-1].Token == SimpleQuoteToken || l.tokens[len(l.tokens)-1].Token == DoubleQuoteToken {
		return false
	}
	i++
	t := Token{
		Token:  ArgToken,
		Lexeme: "?",
	}
	l.tokens = append(l.tokens, t)
	l.pos = i
	return true
}

func (l *lexer) MatchNamedArgToken() bool {

	i := l.pos
	if l.instruction[i] != ':' {
		return false
	}
	i++
	for i < l.instructionLen && unicode.IsLetter(rune(l.instruction[i])) {
		i++
	}
	if i > l.pos+1 {
		t := Token{
			Token:  NamedArgToken,
			Lexeme: string(l.instruction[l.pos+1 : i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchArgToken() bool {

	i := l.pos
	if l.instruction[i] != '$' {
		return false
	}
	i++
	for i < l.instructionLen && unicode.IsDigit(rune(l.instruction[i])) {
		i++
	}
	if i > l.pos+1 {
		t := Token{
			Token:  ArgToken,
			Lexeme: string(l.instruction[l.pos+1 : i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
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

func (l *lexer) genericStringMatcher(str string, token int) Matcher {
	return func() bool {
		return l.Match([]byte(str), token)
	}
}

func (l *lexer) genericByteMatcher(r byte, token int) Matcher {
	return func() bool {
		return l.MatchSingle(r, token)
	}
}

func (l *lexer) MatchAutoincrementToken() bool {
	if l.Match([]byte("auto_increment"), AutoincrementToken) {
		return true
	}

	return l.Match([]byte("autoincrement"), AutoincrementToken)
}

func (l *lexer) MatchStringToken() bool {

	i := l.pos
	for i < l.instructionLen &&
		(unicode.IsLetter(rune(l.instruction[i])) ||
			unicode.IsDigit(rune(l.instruction[i])) ||
			l.instruction[i] == '_' ||
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

func (l *lexer) MatchFloatToken() bool {

	i := l.pos
	for i < l.instructionLen && (unicode.IsDigit(rune(l.instruction[i]))) {
		i++
	}
	if i == l.pos || i >= l.instructionLen {
		return false
	}

	if l.instruction[i] != '.' && l.instruction[i] != 'e' {
		return false
	}

	if l.instruction[i] == '.' {
		i++
	}

	if l.instruction[i] == 'e' {
		i++
		if i >= l.instructionLen {
			return false
		}
		if l.instruction[i] != '+' && l.instruction[i] != '-' {
			return false
		}
		i++
	}

	for i < l.instructionLen && (unicode.IsDigit(rune(l.instruction[i]))) {
		i++
	}

	if i != l.pos {
		t := Token{
			Token:  FloatToken,
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

	for i+1 < l.instructionLen && !(l.instruction[i] == '$' && l.instruction[i+1] == '$') {
		i++
	}
	i++

	if i == l.instructionLen {
		return false
	}

	tok := NumberToken
	escaped := l.instruction[l.pos+2 : i-1]

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
	l.pos = i + 1

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
		if unicode.ToLower(rune(l.instruction[l.pos+i])) != unicode.ToLower(rune(str[i])) {
			return false
		}
	}

	// if next character is still a string, it means it doesn't match
	// ie: COUNT shoulnd match COUNTRY
	if l.instructionLen > l.pos+len(str) {
		if unicode.IsLetter(rune(l.instruction[l.pos+len(str)])) ||
			l.instruction[l.pos+len(str)] == '_' {
			return false
		}
	}

	t := Token{
		Token:  token,
		Lexeme: string(str),
	}

	l.tokens = append(l.tokens, t)
	l.pos += len(t.Lexeme)
	return true
}

func TypeNameFromToken(tk int) string {
	switch tk {
	case IntToken, NumberToken:
		return "int"
	case DateToken:
		return "date"
	case TextToken, StringToken:
		return "text"
	case FloatToken:
		return "float"
	default:
		return "unknown"
	}
}
