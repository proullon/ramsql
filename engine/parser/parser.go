// Package parser implements a parser for SQL statements
//
// Inspired by go/parser
package parser

import (
	"fmt"

	"github.com/proullon/ramsql/engine/log"
)

// The parser structure holds the parser's internal state.
type parser struct {
	i        []Instruction
	index    int
	tokenLen int
	tokens   []Token
}

// Decl structure is the node to statement declaration tree
type Decl struct {
	Token  int
	Lexeme string
	Decl   []*Decl
}

// Stringy prints the declaration tree in console
func (d Decl) Stringy(depth int, printer func(fmt string, varargs ...any)) {

	if printer == nil {
		printer = log.Debug
	}

	indent := ""
	for i := 0; i < depth; i++ {
		indent = fmt.Sprintf("%s    ", indent)
	}

	printer("%s|-> %s (%d)\n", indent, d.Lexeme, d.Token)
	for _, subD := range d.Decl {
		subD.Stringy(depth+1, printer)
	}
}

func (d Decl) Has(t int) (*Decl, bool) {
	for _, child := range d.Decl {
		if child.Token == t {
			return child, true
		}
		if cd, ok := child.Has(t); ok {
			return cd, true
		}
	}
	return nil, false
}

// Instruction define a valid SQL statement
type Instruction struct {
	Decls []*Decl
}

// PrettyPrint prints instruction's declarations on console with indentation
func (i Instruction) PrettyPrint(printer func(fmt string, varargs ...interface{})) {
	for _, d := range i.Decls {
		d.Stringy(0, printer)
	}
}

// NewDecl initialize a Decl struct from a given token
func NewDecl(t Token) *Decl {
	return &Decl{
		Token:  t.Token,
		Lexeme: t.Lexeme,
	}
}

// Add creates a new leaf with given Decl
func (d *Decl) Add(subDecl *Decl) {
	d.Decl = append(d.Decl, subDecl)
}

func (p *parser) parse(tokens []Token) ([]Instruction, error) {
	tokens = stripSpaces(tokens)
	p.tokens = tokens
	log.Debug("parser.parse: %v\n", p.tokens)

	p.tokenLen = len(tokens)
	p.index = 0
	for p.hasNext() {
		// Found a new instruction
		if tokens[p.index].Token == SemicolonToken {
			p.index++
			continue
		}

		// Ignore space token, not needed anymore
		if tokens[p.index].Token == SpaceToken {
			p.index++
			continue
		}

		// Now,
		// Create a logical tree of all tokens
		// We start with first order query
		// CREATE, SELECT, INSERT, UPDATE, DELETE, TRUNCATE, DROP, EXPLAIN
		switch tokens[p.index].Token {
		case CreateToken:
			i, err := p.parseCreate(tokens)
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case SelectToken:
			i, err := p.parseSelect(tokens)
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case InsertToken:
			i, err := p.parseInsert()
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case UpdateToken:
			i, err := p.parseUpdate()
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case DeleteToken:
			i, err := p.parseDelete()
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case TruncateToken:
			i, err := p.parseTruncate()
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case DropToken:
			i, err := p.parseDrop(tokens)
			if err != nil {
				return nil, err
			}
			p.i = append(p.i, *i)
			break
		case ExplainToken:
			break
		case GrantToken:
			i := &Instruction{}
			i.Decls = append(i.Decls, NewDecl(Token{Token: GrantToken}))
			p.i = append(p.i, *i)
			return p.i, nil
		default:
			return nil, fmt.Errorf("Parsing error near <%s>", tokens[p.index].Lexeme)
		}
	}

	return p.i, nil
}

func (p *parser) parseUpdate() (*Instruction, error) {
	i := &Instruction{}

	// Set DELETE decl
	updateDecl, err := p.consumeToken(UpdateToken)
	if err != nil {
		return nil, err
	}
	i.Decls = append(i.Decls, updateDecl)

	// should be table name
	nameDecl, err := p.parseQuotedToken()
	if err != nil {
		return nil, err
	}
	updateDecl.Add(nameDecl)

	// should be SET
	setDecl, err := p.consumeToken(SetToken)
	if err != nil {
		return nil, err
	}
	updateDecl.Add(setDecl)

	// should be a list of equality
	gotClause := false
	for p.tokens[p.index].Token != WhereToken {

		if !p.hasNext() && gotClause {
			break
		}

		attributeDecl, err := p.parseAttribution()
		if err != nil {
			return nil, err
		}
		setDecl.Add(attributeDecl)
		p.consumeToken(CommaToken)

		// Got at least one clause
		gotClause = true
	}

	err = p.parseWhere(updateDecl)
	if err != nil {
		return nil, err
	}

	return i, nil
}

func (p *parser) parseType() (*Decl, error) {
	typeDecl, err := p.consumeToken(FloatToken, DateToken, DecimalToken, NumberToken, StringToken)
	if err != nil {
		return nil, err
	}

	// Maybe a complex type
	if p.is(BracketOpeningToken) {
		_, err = p.consumeToken(BracketOpeningToken)
		if err != nil {
			return nil, err
		}
		sizeDecl, err := p.consumeToken(NumberToken)
		if err != nil {
			return nil, err
		}
		typeDecl.Add(sizeDecl)
		_, err = p.consumeToken(BracketClosingToken)
		if err != nil {
			return nil, err
		}
	}

	return typeDecl, nil
}

func (p *parser) parseOrderBy(selectDecl *Decl) error {
	orderDecl, err := p.consumeToken(OrderToken)
	if err != nil {
		return err
	}
	selectDecl.Add(orderDecl)

	_, err = p.consumeToken(ByToken)
	if err != nil {
		return err
	}

	for {
		// parse attribute now
		attrDecl, err := p.parseAttribute()
		if err != nil {
			return err
		}
		orderDecl.Add(attrDecl)

		if p.is(AscToken, DescToken) {
			decl, err := p.consumeToken(AscToken, DescToken)
			if err != nil {
				return err
			}
			attrDecl.Add(decl)
		}

		if !p.is(CommaToken) {
			break
		}

		if _, err = p.consumeToken(CommaToken); err != nil {
			return nil
		}
	}

	return nil
}

// parseBuiltinFunc looks for COUNT,MAX,MIN
func (p *parser) parseBuiltinFunc() (*Decl, error) {
	var d *Decl
	var err error

	// COUNT(attribute)
	if p.is(CountToken) {
		d, err = p.consumeToken(CountToken)
		if err != nil {
			return nil, err
		}
		// Bracket
		_, err = p.consumeToken(BracketOpeningToken)
		if err != nil {
			return nil, err
		}
		// Attribute
		attr, err := p.parseAttribute()
		if err != nil {
			return nil, err
		}
		d.Add(attr)
		// Bracket
		_, err = p.consumeToken(BracketClosingToken)
		if err != nil {
			return nil, err
		}
	}

	return d, nil
}

// parseTableName parse a table of the form
// schema.table
// "schema".table
// "schema"."table"
// table
func (p *parser) parseTableName() (*Decl, error) {
	quoted := false
	quoteToken := DoubleQuoteToken

	if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
		quoteToken = p.cur().Token
		quoted = true
		if err := p.next(); err != nil {
			return nil, err
		}
	}

	// should be a StringToken here
	// If there is a point after, it's a table name,
	// if not, it's the attribute
	if !p.is(StringToken, StarToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {
		// Check there is a closing quote
		if _, err := p.mustHaveNext(quoteToken); err != nil {
			return nil, err
		}
	}
	quoted = false

	// If no next token, and not quoted, then is was the attribute name
	if err := p.next(); err != nil {
		return decl, nil
	}

	// Now, is it a point ?
	if p.is(PeriodToken) {
		_, err := p.consumeToken(PeriodToken)
		if err != nil {
			return nil, err
		}

		// mayby attribute is quoted as well (see #62)
		if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
			quoteToken = p.cur().Token
			quoted = true
			if err := p.next(); err != nil {
				return nil, err
			}
		}
		// if so, next must be the attribute name or a star
		attributeDecl, err := p.consumeToken(StringToken, StarToken)
		if err != nil {
			return nil, err
		}
		decl.Token = SchemaToken
		attributeDecl.Add(decl)

		if quoted {
			// Check there is a closing quote
			if _, err := p.consumeToken(quoteToken); err != nil {
				return nil, fmt.Errorf("expected closing quote: %s", err)
			}
		}
		return attributeDecl, nil
	}

	// AS SOMETHING ?
	if p.is(AsToken) {
		asDecl, err := p.consumeToken(AsToken)
		if err != nil {
			return nil, err
		}
		decl.Add(asDecl)
		aliasDecl, err := p.consumeToken(StringToken)
		if err != nil {
			return nil, err
		}
		asDecl.Add(aliasDecl)
	}

	// Then the first string token was the naked attribute name
	return decl, nil
}

// parseAttribute parse an attribute of the form
// table.foo
// table.*
// "table".foo
// "table"."foo"
// foo
func (p *parser) parseAttribute() (*Decl, error) {
	quoted := false
	quoteToken := DoubleQuoteToken

	if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
		quoteToken = p.cur().Token
		quoted = true
		if err := p.next(); err != nil {
			return nil, err
		}
	}

	// should be a StringToken here
	// If there is a point after, it's a table name,
	// if not, it's the attribute
	if !p.is(StringToken, StarToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {
		// Check there is a closing quote
		if _, err := p.mustHaveNext(quoteToken); err != nil {
			return nil, err
		}
	}
	quoted = false

	// If no next token, and not quoted, then is was the attribute name
	if err := p.next(); err != nil {
		return decl, nil
	}

	// Now, is it a point ?
	if p.is(PeriodToken) {
		_, err := p.consumeToken(PeriodToken)
		if err != nil {
			return nil, err
		}

		// mayby attribute is quoted as well (see #62)
		if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
			quoteToken = p.cur().Token
			quoted = true
			if err := p.next(); err != nil {
				return nil, err
			}
		}
		// if so, next must be the attribute name or a star
		attributeDecl, err := p.consumeToken(StringToken, StarToken)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)

		if quoted {
			// Check there is a closing quote
			if _, err := p.consumeToken(quoteToken); err != nil {
				return nil, fmt.Errorf("expected closing quote: %s", err)
			}
		}
		return attributeDecl, nil
	}

	// Then the first string token was the naked attribute name
	return decl, nil
}

// parseQuotedToken parse a token of the form
// table
// "table"
func (p *parser) parseQuotedToken() (*Decl, error) {
	quoted := false
	quoteToken := DoubleQuoteToken

	if p.is(DoubleQuoteToken) || p.is(BacktickToken) {
		quoted = true
		quoteToken = p.cur().Token
		if err := p.next(); err != nil {
			return nil, err
		}
	}

	// shoud be a StringToken here
	if !p.is(StringToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {

		// Check there is a closing quote
		if _, err := p.mustHaveNext(quoteToken); err != nil {
			return nil, err
		}
	}

	p.next()
	return decl, nil
}

func (p *parser) parseAttribution() (*Decl, error) {

	// Attribute
	attributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}

	// Equals operator
	if p.cur().Token == EqualityToken {
		decl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
	}

	// Value
	if p.cur().Token == NullToken {
		nullDecl, err := p.consumeToken(NullToken)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(nullDecl)
	} else {
		valueDecl, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(valueDecl)
	}

	return attributeDecl, nil
}

func (p *parser) parseIn() (*Decl, error) {
	inDecl, err := p.consumeToken(InToken)
	if err != nil {
		return nil, err
	}

	// bracket opening
	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}

	// list of value
	gotList := false
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		inDecl.Add(v)
		gotList = true

		if p.is(BracketClosingToken) {
			if gotList == false {
				return nil, fmt.Errorf("IN clause: empty list of value")
			}
			p.consumeToken(BracketClosingToken)
			break
		}

		_, err = p.consumeToken(CommaToken)
		if err != nil {
			return nil, err
		}
	}

	return inDecl, nil
}

func (p *parser) parseValue() (*Decl, error) {
	quoted := false

	if p.is(SimpleQuoteToken) || p.is(DoubleQuoteToken) {
		quoted = true
		_, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
		if err != nil {
			return nil, err
		}
	}

	valueDecl, err := p.consumeToken(FloatToken, StringToken, NumberToken, DateToken, NowToken, CurrentSchemaToken, ArgToken, NamedArgToken)
	if err != nil {
		return nil, err
	}

	if quoted {
		_, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
		if err != nil {
			return nil, err
		}
	}

	return valueDecl, nil
}

func (p *parser) parseStringLiteral() (*Decl, error) {
	singleQuoted := p.is(SimpleQuoteToken)
	_, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
	if err != nil {
		return nil, err
	}
	valueDecl, err := p.consumeToken(StringToken)
	if err != nil {
		return nil, err
	}
	if (singleQuoted && p.is(DoubleQuoteToken)) || (!singleQuoted && p.is(SimpleQuoteToken)) {
		return nil, fmt.Errorf("Quotation marks do not match.")
	}
	_, err = p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
	if err != nil {
		return nil, err
	}
	return valueDecl, nil
}

// parseJoin parses the JOIN keywords and all its condition
// JOIN user_addresses ON address.id=user_addresses.address_id
func (p *parser) parseJoin() (*Decl, error) {
	joinDecl, err := p.consumeToken(JoinToken)
	if err != nil {
		return nil, err
	}

	// TABLE NAME
	tableDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}
	joinDecl.Add(tableDecl)

	// AS SOMETHING ?
	if p.is(AsToken) {
		p.consumeToken(AsToken)
		aliasDecl, err := p.consumeToken(StringToken)
		if err != nil {
			return nil, err
		}
		tableDecl.Add(aliasDecl)
	}

	// ON
	onDecl, err := p.consumeToken(OnToken)
	if err != nil {
		return nil, err
	}
	// onDecl := NewDecl(t)
	joinDecl.Add(onDecl)

	// ATTRIBUTE
	leftAttributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}
	onDecl.Add(leftAttributeDecl)

	// EQUAL
	equalAttr, err := p.consumeToken(EqualityToken)
	if err != nil {
		return nil, err
	}
	onDecl.Add(equalAttr)

	//ATTRIBUTE
	rightAttributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}
	onDecl.Add(rightAttributeDecl)

	return joinDecl, nil
}

func (p *parser) next() error {
	if !p.hasNext() {
		return fmt.Errorf("Unexpected end")
	}
	p.index++
	return nil
}

func (p *parser) hasNext() bool {
	if p.index+1 < len(p.tokens) {
		return true
	}
	return false
}

func (p *parser) is(tokenTypes ...int) bool {

	for _, tokenType := range tokenTypes {
		if p.cur().Token == tokenType {
			return true
		}
	}

	return false
}

func (p *parser) isNot(tokenTypes ...int) bool {
	return !p.is(tokenTypes...)
}

func (p *parser) isNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		return t, p.syntaxError()
	}

	for _, tokenType := range tokenTypes {
		if p.tokens[p.index+1].Token == tokenType {
			return p.tokens[p.index+1], nil
		}
	}

	return t, p.syntaxError()
}

func (p *parser) mustHaveNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		return t, p.syntaxError()
	}

	if err = p.next(); err != nil {
		return t, err
	}

	for _, tokenType := range tokenTypes {
		if p.is(tokenType) {
			return p.tokens[p.index], nil
		}
	}

	return t, p.syntaxError()
}

func (p *parser) cur() Token {
	return p.tokens[p.index]
}

func (p *parser) consumeToken(tokenTypes ...int) (*Decl, error) {
	if !p.is(tokenTypes...) {
		return nil, p.syntaxError()
	}

	decl := NewDecl(p.tokens[p.index])
	p.next()
	return decl, nil
}

func (p *parser) syntaxError() error {
	if p.index == 0 {
		return fmt.Errorf("Syntax error near %v %v", p.tokens[p.index].Lexeme, p.tokens[p.index+1].Lexeme)
	} else if !p.hasNext() {
		return fmt.Errorf("Syntax error near %v %v", p.tokens[p.index-1].Lexeme, p.tokens[p.index].Lexeme)
	}
	return fmt.Errorf("Syntax error near %v %v %v", p.tokens[p.index-1].Lexeme, p.tokens[p.index].Lexeme, p.tokens[p.index+1].Lexeme)
}

func stripSpaces(t []Token) (ret []Token) {
	for i := range t {
		if t[i].Token != SpaceToken {
			ret = append(ret, t[i])
		}
	}
	return ret
}
