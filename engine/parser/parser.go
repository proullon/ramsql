// Parser package implements a parser for SQL statements
//
// Inspired by go/parser
package parser

import (
	"fmt"
	"log"
)

// The parser structure holds the parser's internal state.
type parser struct {
	i        []Instruction
	index    int
	tokenLen int
	tokens   []Token
}

type Decl struct {
	Token  int
	Lexeme string
	Decl   []*Decl
}

func (d Decl) Stringy(depth int) {
	indent := ""
	for i := 0; i < depth; i++ {
		indent = fmt.Sprintf("%s    ", indent)
	}

	fmt.Printf("%s|-> %s\n", indent, d.Lexeme)
	for _, subD := range d.Decl {
		subD.Stringy(depth + 1)
	}
}

type Instruction struct {
	Decls []*Decl
}

func (i Instruction) PrettyPrint() {
	for _, d := range i.Decls {
		d.Stringy(0)
	}
}

func NewDecl(t Token) *Decl {
	return &Decl{
		Token:  t.Token,
		Lexeme: t.Lexeme,
	}
}

func (d *Decl) Add(subDecl *Decl) {
	d.Decl = append(d.Decl, subDecl)
}

func (p *parser) parse(tokens []Token) ([]Instruction, error) {
	tokens = stripSpaces(tokens)
	p.tokens = tokens
	log.Printf("parser.parse : %v", tokens)

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
			break
		case UpdateToken:
			break
		case DeleteToken:
			break
		case ExplainToken:
			break
		default:
			return nil, fmt.Errorf("Parsing error near <%s>", tokens[p.index].Lexeme)
		}
	}

	return p.i, nil
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
	// fmt.Printf("Index : %d\n", p.index)
	p.index++
	// fmt.Printf("Index : %d\n", p.index)

	switch tokens[p.index].Token {
	case TableToken:
		d, err := p.parseTable(tokens)
		if err == nil {
			createDecl.Add(d)
		}
		break
	default:
		return nil, fmt.Errorf("Parsing error near <%s>", tokens[p.index].Lexeme)
	}

	return i, nil
}

func (p *parser) parseTable(tokens []Token) (*Decl, error) {
	log.Printf("parser.parseTable")
	var err error
	tableDecl := NewDecl(tokens[p.index])
	p.index++

	// Now we should found table name
	if !p.hasNext() || tokens[p.index].Token != StringToken {
		return nil, fmt.Errorf("TABLE token must be followed by table name")
	}
	nameTable := NewDecl(tokens[p.index])
	tableDecl.Add(nameTable)
	p.index++

	// Now we should found brackets
	if !p.hasNext() || tokens[p.index].Token != BracketOpeningToken {
		return nil, fmt.Errorf("Table name token must be followed by table definition")
	}
	p.index++

	for p.index < len(tokens) {
		// New attribute name
		if tokens[p.index].Token != StringToken {
			return nil, fmt.Errorf("Expected attribute name, not <%s>", tokens[p.index].Lexeme)
		}
		newAttribute := NewDecl(tokens[p.index])
		tableDecl.Add(newAttribute)
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end")
		}

		// New attribute type
		if tokens[p.index].Token != StringToken {
			return nil, fmt.Errorf("Expected attribute type, not <%s>", tokens[p.index].Lexeme)
		}
		newAttributeType := NewDecl(tokens[p.index])
		newAttribute.Add(newAttributeType)
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end")
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

		// Closing bracket ?
		if tokens[p.index].Token == BracketClosingToken {
			p.index++
			break
		}

		// Then comma ?
		if tokens[p.index].Token != CommaToken {
			return nil, fmt.Errorf("Missing coma")
		}
		p.index++
	}

	return tableDecl, nil
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
	if err = p.next(); err != nil {
		return nil, fmt.Errorf("SELECT token must be followed by attributes to select")
	}
	if tokens[p.index].Token == StarToken {
		starDecl := NewDecl(tokens[p.index])
		selectDecl.Add(starDecl)
	}
	for {
		if err = p.next(); err != nil {
			return nil, fmt.Errorf("Unexpected end. Syntax error near %v\n", tokens[p.index])
		}
		if !p.is(StringToken) {
			break // No more selected thing
		}
		// Add declaration
		// Next must be a point or a comma
		// If point, must be String or Star
		// If comma, loop again
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
			return nil, syntaxError(tokens[p.index])
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

	// Must be WHERE OR ... here
	log.Printf("WHERE ? %v\n", tokens[p.index])
	if tokens[p.index].Token != WhereToken {
		return nil, syntaxError(tokens[p.index])
	}
	whereDecl := NewDecl(tokens[p.index])
	selectDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Equal and Value
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

func (p *parser) parseCondition() (*Decl, error) {
	if !p.hasNext() {
		return nil, fmt.Errorf("Unexpected end, expected condition clause")
	}

	// Attribute
	t, err := p.mustHaveNext(StringToken)
	if err != nil {
		return nil, err
	}
	attributeDecl := NewDecl(t)

	// Equal
	t, err = p.mustHaveNext(EqualityToken)
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(NewDecl(t))

	// Value
	valueDecl, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(valueDecl)

	return attributeDecl, nil
}

func (p *parser) parseValue() (*Decl, error) {
	log.Printf("parseValue")
	defer log.Printf("~parseValue")
	quoted := false

	if err := p.next(); err != nil {
		return nil, err
	}

	if p.is(QuoteToken) {
		log.Printf("value is quoted")
		quoted = true
	}

	t, err := p.mustHaveNext(StringToken)
	if err != nil {
		return nil, err
	}
	valueDecl := NewDecl(t)

	if quoted {
		if _, err := p.mustHaveNext(QuoteToken); err != nil {
			return nil, err
		}
	}

	return valueDecl, nil
}

func (p *parser) next() error {
	log.Printf("parser.next: current is %v", p.tokens[p.index])
	if !p.hasNext() {
		return fmt.Errorf("Unexpected end")
	}
	p.index += 1
	log.Printf("parser.next: next is %v", p.tokens[p.index])
	return nil
}

func (p *parser) hasNext() bool {
	log.Printf("parser.hasNext : Len is %d, current index is %d", len(p.tokens), p.index)
	if p.index+1 < len(p.tokens) {
		return true
	}
	return false
}

func (p *parser) is(tokenType int) bool {
	if p.tokens[p.index].Token == tokenType {
		return true
	}

	return false
}

func (p *parser) mustHaveNext(tokenTypes ...int) (t Token, err error) {
	log.Printf("parser.mustHaveNext")
	defer log.Printf("~parser.mustHaveNext")

	if !p.hasNext() {
		log.Printf("parser.mustHaveNext: has no next")
		return t, syntaxError(t)
	}

	if err = p.next(); err != nil {
		log.Printf("parser.mustHaveNext: error getting next")
		return t, err
	}

	for _, tokenType := range tokenTypes {
		if p.is(tokenType) {
			return p.tokens[p.index], nil
		}
	}

	log.Printf("parser.mustHaveNext: Next is not among %v", tokenTypes)
	return t, syntaxError(p.tokens[p.index])
}

// func hasNext(t []Token, index int) bool {
// 	if len(t) > index {
// 		return true
// 	}

// 	return false
// }

// func next(tokens []Token, index int) (int, error) {
// 	if !p.hasNext(tokens, index) {
// 		return index, fmt.Errorf("Unexpected end")
// 	}
// 	return index + 1, nil
// }

func stripSpaces(t []Token) (ret []Token) {
	for i := range t {
		if t[i].Token != SpaceToken {
			ret = append(ret, t[i])
		}
	}
	return ret
}

func syntaxError(t Token) error {
	return fmt.Errorf("Syntax error near %v\n", t)
}
