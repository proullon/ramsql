package parser

import (
	"github.com/proullon/ramsql/engine/log"
)

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
	nameDecl, err := p.parseTableName()
	if err != nil {
		return nil, err
	}
	fromDecl.Add(nameDecl)

	log.Debug("WHERE ? %v", p.tokens[p.index])
	if !p.hasNext() {
		return i, nil
	}

	err = p.parseWhere(deleteDecl)
	if err != nil {
		return nil, err
	}

	return i, nil
}
