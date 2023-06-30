package parser

func (p *parser) parseDrop(tokens []Token) (*Instruction, error) {
	var err error
	i := &Instruction{}

	trDecl, err := p.consumeToken(DropToken)
	if err != nil {
		return nil, err
	}
	i.Decls = append(i.Decls, trDecl)

	var d *Decl
	switch tokens[p.index].Token {
	case TableToken:
		d, err = p.consumeToken(TableToken)
		if err != nil {
			return nil, err
		}
		break
	case IndexToken:
		d, err = p.consumeToken(SchemaToken)
		if err != nil {
			return nil, err
		}
		break
	case SchemaToken:
		d, err = p.consumeToken(SchemaToken)
		if err != nil {
			return nil, err
		}
		break
	}
	trDecl.Add(d)

	// Should be a name attribute
	nameDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}
	d.Add(nameDecl)

	return i, nil
}
