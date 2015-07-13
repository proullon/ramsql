package parser

import ()

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
	if !p.hasNext() {
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
