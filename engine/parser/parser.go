// Parser package implements a parser for SQL statements
//
// Inspired by go/parser
package parser

import (
	"log"
)

// The parser structure holds the parser's internal state.
type parser struct {
	i []Instruction
}

type Instruction struct {
	Decls []Decl
}

func (p *parser) parse(tokens []Decl) ([]Instruction, error) {
	log.Printf("parser.parse : %v", tokens)

	i := Instruction{}
	for _, t := range tokens {

		// Found a new instruction
		if t.Token == SemicolonToken {
			p.i = append(p.i, i)
			i = Instruction{}
			continue
		}

		i.Decls = append(i.Decls, t)
	}
	return p.i, nil
}
