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
}

type Decl struct {
	Token
	Decl []*Decl
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
		Token: Token{
			Token:  t.Token,
			Lexeme: t.Lexeme,
		},
	}
}

func (d *Decl) Add(subDecl *Decl) {
	d.Decl = append(d.Decl, subDecl)
}

func (p *parser) parse(tokens []Token) ([]Instruction, error) {
	tokens = stripSpaces(tokens)
	log.Printf("parser.parse : %v", tokens)

	p.tokenLen = len(tokens)
	p.index = 0
	for p.index < p.tokenLen {
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
		// CREATE, INSERT, UPDATE, DELETE, EXPLAIN
		switch tokens[p.index].Token {
		case CreateToken:
			i, err := p.parseCreate(tokens)
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
	if !hasNext(tokens, p.index) {
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
	if !hasNext(tokens, p.index) || tokens[p.index].Token != StringToken {
		return nil, fmt.Errorf("TABLE token must be followed by table name")
	}
	nameTable := NewDecl(tokens[p.index])
	tableDecl.Add(nameTable)
	p.index++

	// Now we should found brackets
	if !hasNext(tokens, p.index) || tokens[p.index].Token != BracketOpeningToken {
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
		if p.index, err = next(tokens, p.index); err != nil {
			return nil, fmt.Errorf("Unexpected end")
		}

		// New attribute type
		if tokens[p.index].Token != StringToken {
			return nil, fmt.Errorf("Expected attribute type, not <%s>", tokens[p.index].Lexeme)
		}
		newAttributeType := NewDecl(tokens[p.index])
		newAttribute.Add(newAttributeType)
		if p.index, err = next(tokens, p.index); err != nil {
			return nil, fmt.Errorf("Unexpected end")
		}

		// Is it a primary key ?
		if tokens[p.index].Token == PrimaryToken && hasNext(tokens, p.index+1) && tokens[p.index+1].Token == KeyToken {
			newPrimary := NewDecl(tokens[p.index])
			newAttribute.Add(newPrimary)

			if p.index, err = next(tokens, p.index); err != nil {
				return nil, fmt.Errorf("Unexpected end")
			}

			newKey := NewDecl(tokens[p.index])
			newPrimary.Add(newKey)

			if p.index, err = next(tokens, p.index); err != nil {
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

func hasNext(t []Token, index int) bool {
	if len(t) > index {
		return true
	}

	return false
}

func next(tokens []Token, index int) (int, error) {
	if !hasNext(tokens, index) {
		return index, fmt.Errorf("Unexpected end")
	}
	return index + 1, nil
}

func stripSpaces(t []Token) (ret []Token) {
	for i := range t {
		if t[i].Token != SpaceToken {
			ret = append(ret, t[i])
		}
	}
	return ret
}
