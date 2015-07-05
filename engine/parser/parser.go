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
func (d Decl) Stringy(depth int) {
	indent := ""
	for i := 0; i < depth; i++ {
		indent = fmt.Sprintf("%s    ", indent)
	}

	log.Debug("%s|-> %s\n", indent, d.Lexeme)
	for _, subD := range d.Decl {
		subD.Stringy(depth + 1)
	}
}

// Instruction define a valid SQL statement
type Instruction struct {
	Decls []*Decl
}

// PrettyPrint prints instruction's declarations on console with indentation
func (i Instruction) PrettyPrint() {
	for _, d := range i.Decls {
		d.Stringy(0)
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

	p.tokenLen = len(tokens)
	p.index = 0
	for p.hasNext() {
		// fmt.Printf("Token index : %d\n", p.index)

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
		// CREATE, SELECT, INSERT, UPDATE, DELETE, EXPLAIN
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
		case ExplainToken:
			break
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

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		setDecl.Add(attributeDecl)
		p.consumeToken(CommaToken)

		// Got at least one clause
		gotClause = true
	}

	// WHERE
	whereDecl, err := p.consumeToken(WhereToken)
	if err != nil {
		return nil, err
	}
	updateDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Operator and Value
	gotClause = false
	for {
		if !p.hasNext() && gotClause {
			break
		}

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		whereDecl.Add(attributeDecl)

		// Got at least one clause
		gotClause = true
	}

	return i, nil
}

func (p *parser) parseDelete() (*Instruction, error) {
	i := &Instruction{}

	// Set DELETE decl
	deleteDecl, err := p.consumeToken(DeleteToken)
	if err != nil {
		return nil, err
	}
	i.Decls = append(i.Decls, deleteDecl)

	// should be From
	fromDecl, err := p.consumeToken(FromToken)
	if err != nil {
		return nil, err
	}
	deleteDecl.Add(fromDecl)

	// Should be a table name
	nameDecl, err := p.parseQuotedToken()
	if err != nil {
		return nil, err
	}
	fromDecl.Add(nameDecl)

	// MAY be WHERE  here
	debug("WHERE ? %v", p.tokens[p.index])
	if _, err := p.isNext(WhereToken); err != nil {
		return i, nil
	}

	whereDecl, err := p.consumeToken(WhereToken)
	if err != nil {
		return nil, err
	}
	deleteDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Operator and Value
	gotClause := false
	for {
		if !p.hasNext() && gotClause {
			break
		}

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		whereDecl.Add(attributeDecl)

		// Got at least one clause
		gotClause = true
	}

	return i, nil
}

func (p *parser) parseInsert() (*Instruction, error) {
	i := &Instruction{}

	// Set INSERT decl
	insertDecl, err := p.consumeToken(InsertToken)
	if err != nil {
		return nil, err
	}
	i.Decls = append(i.Decls, insertDecl)

	// should be INTO
	intoDecl, err := p.consumeToken(IntoToken)
	if err != nil {
		return nil, err
	}
	insertDecl.Add(intoDecl)

	// should be table Name
	tableDecl, err := p.parseQuotedToken()
	if err != nil {
		return nil, err
	}
	intoDecl.Add(tableDecl)

	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}

	// concerned attribute
	for {
		decl, err := p.parseListElement()
		if err != nil {
			return nil, err
		}
		tableDecl.Add(decl)

		if p.is(BracketClosingToken) {
			if _, err = p.consumeToken(BracketClosingToken); err != nil {
				return nil, err
			}

			break
		}

		_, err = p.consumeToken(CommaToken)
		if err != nil {
			return nil, err
		}
	}

	// should be VALUES
	valuesDecl, err := p.consumeToken(ValuesToken)
	if err != nil {
		return nil, err
	}
	insertDecl.Add(valuesDecl)

	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}

	// should be a list of values for specified attributes
	for {
		decl, err := p.parseListElement()
		if err != nil {
			return nil, err
		}
		valuesDecl.Add(decl)

		if p.is(BracketClosingToken) {
			p.consumeToken(BracketClosingToken)
			break
		}

		_, err = p.consumeToken(CommaToken)
		if err != nil {
			return nil, err
		}
	}

	return i, nil
}

func (p *parser) parseCreate(tokens []Token) (*Instruction, error) {
	i := &Instruction{}

	// Set CREATE decl
	createDecl := NewDecl(tokens[p.index])
	i.Decls = append(i.Decls, createDecl)

	// After create token, should be either
	// TABLE
	// INDEX
	// ...
	if !p.hasNext() {
		return nil, fmt.Errorf("CREATE token must be followed by TABLE, INDEX")
	}
	p.index++

	switch tokens[p.index].Token {
	case TableToken:
		d, err := p.parseTable(tokens)
		if err != nil {
			return nil, err
		}
		createDecl.Add(d)
		break
	default:
		return nil, fmt.Errorf("Parsing error near <%s>", tokens[p.index].Lexeme)
	}

	return i, nil
}

func (p *parser) parseTable(tokens []Token) (*Decl, error) {
	debug("parser.parseTable")
	var err error
	tableDecl := NewDecl(tokens[p.index])
	p.index++

	// Maybe have "IF NOT EXISTS" here
	if p.is(IfToken) {
		ifDecl, err := p.consumeToken(IfToken)
		if err != nil {
			return nil, err
		}
		tableDecl.Add(ifDecl)

		if p.is(NotToken) {
			notDecl, err := p.consumeToken(NotToken)
			if err != nil {
				return nil, err
			}
			ifDecl.Add(notDecl)
			if !p.is(ExistsToken) {
				return nil, p.syntaxError()
			}
			existsDecl, err := p.consumeToken(ExistsToken)
			if err != nil {
				return nil, err
			}
			notDecl.Add(existsDecl)
		}
	}

	// Now we should found table name
	nameTable, err := p.parseAttribute()
	if err != nil {
		return nil, p.syntaxError()
	}
	tableDecl.Add(nameTable)

	// Now we should found brackets
	if !p.hasNext() || tokens[p.index].Token != BracketOpeningToken {
		return nil, fmt.Errorf("Table name token must be followed by table definition")
	}
	p.index++

	for p.index < len(tokens) {
		// New attribute name
		newAttribute, err := p.parseQuotedToken()
		if err != nil {
			return nil, err
		}
		tableDecl.Add(newAttribute)

		newAttributeType, err := p.parseType()
		if err != nil {
			return nil, err
		}
		newAttribute.Add(newAttributeType)

		// Is it not null ?
		if _, err = p.isNext(NullToken); p.is(NotToken) && err == nil {
			notDecl, err := p.consumeToken(NotToken)
			if err != nil {
				return nil, err
			}
			newAttribute.Add(notDecl)
			nullDecl, err := p.consumeToken(NullToken)
			if err != nil {
				return nil, err
			}
			notDecl.Add(nullDecl)
		}

		// Is it a primary key ?
		if tokens[p.index].Token == PrimaryToken && p.hasNext() && tokens[p.index+1].Token == KeyToken {
			newPrimary := NewDecl(tokens[p.index])
			newAttribute.Add(newPrimary)

			if err = p.next(); err != nil {
				return nil, fmt.Errorf("Unexpected end")
			}

			newKey := NewDecl(tokens[p.index])
			newPrimary.Add(newKey)

			if err = p.next(); err != nil {
				return nil, fmt.Errorf("Unexpected end")
			}
		}

		// ANOTHER PROPERTY FFS ! Autoincrement ?
		if p.is(AutoincrementToken) {
			autoincDecl, err := p.consumeToken(AutoincrementToken)
			if err != nil {
				return nil, err
			}
			newAttribute.Add(autoincDecl)
		}

		// Closing bracket ?
		if tokens[p.index].Token == BracketClosingToken {
			p.index++
			break
		}

		// Then comma ?
		if tokens[p.index].Token != CommaToken {
			return nil, p.syntaxError()
			// return nil, fmt.Errorf("Missing coma")
		}
		p.index++
	}

	debug("OUT parseTable")
	return tableDecl, nil
}

func (p *parser) parseType() (*Decl, error) {
	typeDecl, err := p.consumeToken(StringToken)
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

func (p *parser) parseSelect(tokens []Token) (*Instruction, error) {
	i := &Instruction{}
	var err error

	// Create select decl
	selectDecl := NewDecl(tokens[p.index])
	i.Decls = append(i.Decls, selectDecl)

	// After select token, should be either
	// a StarToken
	// a list of table names + (StarToken Or Attribute)
	// a builtin func (COUNT, MAX, ...)
	if err = p.next(); err != nil {
		return nil, fmt.Errorf("SELECT token must be followed by attributes to select")
	}

	for {
		if p.is(CountToken) {
			attrDecl, err := p.parseBuiltinFunc()
			if err != nil {
				return nil, err
			}
			selectDecl.Add(attrDecl)
		} else {
			attrDecl, err := p.parseAttribute()
			if err != nil {
				return nil, err
			}
			selectDecl.Add(attrDecl)
		}

		// If comma, loop again.
		if p.is(CommaToken) {
			if err := p.next(); err != nil {
				return nil, err
			}
			continue
		}
		break
	}

	// Must be from now
	if tokens[p.index].Token != FromToken {
		return nil, fmt.Errorf("Syntax error near %v\n", tokens[p.index])
	}
	fromDecl := NewDecl(tokens[p.index])
	selectDecl.Add(fromDecl)

	// Now must be a list of table
	for {
		// string
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end. Syntax error near %v\n", tokens[p.index])
		}
		if tokens[p.index].Token != StringToken {
			return nil, p.syntaxError()
		}
		tableNameDecl := NewDecl(tokens[p.index])
		fromDecl.Add(tableNameDecl)

		// if not comma, break
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end. Syntax error near %v\n", tokens[p.index])
		}
		if tokens[p.index].Token != CommaToken {
			break // No more table
		}
	}

	// JOIN OR ...?
	if p.is(JoinToken) {
		joinDecl, err := p.parseJoin()
		if err != nil {
			return nil, err
		}
		selectDecl.Add(joinDecl)
	}

	// Must be WHERE  here
	whereDecl, err := p.consumeToken(WhereToken)
	if err != nil {
		return nil, err
	}
	selectDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Operator and Value
	gotClause := false
	for {
		if !p.hasNext() && gotClause {
			break
		}

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return nil, err
		}
		whereDecl.Add(attributeDecl)

		// Got at least one clause
		gotClause = true
	}

	return i, nil
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

// parseAttribute parse an attribute of the form
// table.foo
// table.*
// "table".foo
// foo
func (p *parser) parseAttribute() (*Decl, error) {
	quoted := false

	if p.is(DoubleQuoteToken) {
		quoted = true
		if err := p.next(); err != nil {
			return nil, err
		}
	}

	// shoud be a StringToken here
	// If there is a point after, it's a table name,
	// if not, it's the attribute
	if !p.is(StringToken, StarToken) {
		return nil, p.syntaxError()
	}
	decl := NewDecl(p.cur())

	if quoted {
		// Check there is a closing quote
		if _, err := p.mustHaveNext(DoubleQuoteToken); err != nil {
			debug("parseAttribute: Missing closing quote")
			return nil, err
		}
	}
	if err := p.next(); err != nil {
		return nil, err
	}

	// Now, is it a point ?
	if p.is(PeriodToken) {
		// if so, next must be the attribute name or a star
		t, err := p.mustHaveNext(StringToken, StarToken)
		if err != nil {
			return nil, err
		}
		attributeDecl := NewDecl(t)
		attributeDecl.Add(decl)
		return attributeDecl, p.next()
	}

	// Then the first string token was the naked attribute name
	return decl, nil
}

// parseQuotedToken parse a token of the form
// table
// "table"
func (p *parser) parseQuotedToken() (*Decl, error) {
	quoted := false

	if p.is(DoubleQuoteToken) {
		quoted = true
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
		if _, err := p.mustHaveNext(DoubleQuoteToken); err != nil {
			return nil, err
		}
	}

	p.next()
	return decl, nil
}

func (p *parser) parseCondition() (*Decl, error) {

	// We may have the WHERE 1 condition
	if t := p.cur(); t.Token == NumberToken && t.Lexeme == "1" {
		attributeDecl := NewDecl(t)
		p.next()
		return attributeDecl, nil
	}

	// Attribute
	attributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}

	// Equal
	/*if !p.is(EqualityToken) {
		return nil, p.syntaxError()
	}
	equalDecl := NewDecl(p.cur())*/
	equalDecl, err := p.consumeToken(EqualityToken)
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(equalDecl)

	// Value
	valueDecl, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(valueDecl)

	return attributeDecl, nil
}

func (p *parser) parseValue() (*Decl, error) {
	debug("parseValue")
	defer debug("~parseValue")
	quoted := false

	/*if _, err := p.isNext(SimpleQuoteToken, DoubleQuoteToken); err == nil {
		p.next()
		quoted = true
		debug("value is quoted!")
	}*/
	if p.is(SimpleQuoteToken) || p.is(DoubleQuoteToken) {
		quoted = true
		debug("value %v is quoted!", p.tokens[p.index])
		_, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
		if err != nil {
			return nil, err
		}
	}

	/*t, err := p.mustHaveNext(StringToken, NumberToken)
	if err != nil {
		return nil, err
	}
	valueDecl := NewDecl(t)*/
	valueDecl, err := p.consumeToken(StringToken, NumberToken)
	if err != nil {
		debug("parseValue: Wasn't expecting %v\n", p.tokens[p.index])
		return nil, err
	}
	debug("Parsing value %v !\n", valueDecl)

	if quoted {
		/*if _, err := p.mustHaveNext(SimpleQuoteToken, DoubleQuoteToken); err != nil {
			return nil, err
		}*/
		debug("consume quote %v\n", p.tokens[p.index])
		_, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken)
		if err != nil {
			debug("uuuh, wasn't a quote")
			return nil, err
		}
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

func (p *parser) parseListElement() (*Decl, error) {
	quoted := false

	if p.is(SimpleQuoteToken) || p.is(DoubleQuoteToken) {
		quoted = true
		p.next()
	}

	var valueDecl *Decl
	valueDecl, err := p.consumeToken(StringToken, NumberToken, NullToken)
	if err != nil {
		return nil, err
	}

	if quoted {
		if _, err := p.consumeToken(SimpleQuoteToken, DoubleQuoteToken); err != nil {
			return nil, err
		}
	}

	return valueDecl, nil
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

func (p *parser) isNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		debug("parser.isNext: has no next")
		return t, p.syntaxError()
	}

	debug("parser.isNext %v", tokenTypes)
	for _, tokenType := range tokenTypes {
		if p.tokens[p.index+1].Token == tokenType {
			return p.tokens[p.index+1], nil
		}
	}

	debug("parser.isNext: Next (%v) is not among %v", p.cur(), tokenTypes)
	return t, p.syntaxError()
}

func (p *parser) mustHaveNext(tokenTypes ...int) (t Token, err error) {

	if !p.hasNext() {
		debug("parser.mustHaveNext: has no next")
		return t, p.syntaxError()
	}

	if err = p.next(); err != nil {
		debug("parser.mustHaveNext: error getting next")
		return t, err
	}

	debug("parser.mustHaveNext %v", tokenTypes)
	for _, tokenType := range tokenTypes {
		if p.is(tokenType) {
			return p.tokens[p.index], nil
		}
	}

	debug("parser.mustHaveNext: Next (%v) is not among %v", p.cur(), tokenTypes)
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
