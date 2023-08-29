package parser

import (
	"fmt"
	"strings"
)

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
	case IndexToken:
		d, err := p.parseIndex(tokens)
		if err != nil {
			return nil, err
		}
		createDecl.Add(d)
		break
	case SchemaToken:
		d, err := p.parseSchema(tokens)
		if err != nil {
			return nil, err
		}
		createDecl.Add(d)
		break
	case UniqueToken:
		u, err := p.consumeToken(UniqueToken)
		if err != nil {
			return nil, err
		}
		// should have index after unique here
		if !p.hasNext() || tokens[p.index].Token != IndexToken {
			return nil, fmt.Errorf("expected INDEX after UNIQUE")
		}
		d, err := p.parseIndex(tokens)
		if err != nil {
			return nil, err
		}
		d.Add(u)
		createDecl.Add(d)
		break

	default:
		return nil, fmt.Errorf("Parsing error near <%s>", tokens[p.index].Lexeme)
	}

	return i, nil
}

// INDEX index_name ON table_name (col1, col2)
func (p *parser) parseIndex(tokens []Token) (*Decl, error) {
	var err error
	indexDecl := NewDecl(tokens[p.index])
	p.index++

	// Maybe have "IF NOT EXISTS" here
	if p.is(IfToken) {
		ifDecl, err := p.consumeToken(IfToken)
		if err != nil {
			return nil, err
		}
		indexDecl.Add(ifDecl)

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

	// Now we should found index name
	nameIndex, err := p.parseAttribute()
	if err != nil {
		return nil, p.syntaxError()
	}
	indexDecl.Add(nameIndex)

	// ON
	if !p.hasNext() || tokens[p.index].Token != OnToken {
		return nil, fmt.Errorf("Expected ON")
	}
	p.index++

	// Now we should found table name
	nameTable, err := p.parseTableName()
	if err != nil {
		return nil, p.syntaxError()
	}
	nameTable.Token = TableToken
	indexDecl.Add(nameTable)

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
		indexDecl.Add(newAttribute)

		// Closing bracket ?
		if tokens[p.index].Token == BracketClosingToken {
			p.consumeToken(BracketClosingToken)
			break
		}

		// All the following tokens until bracket or comma are column constraints.
		// Column constraints can be listed in any order.
		for p.isNot(BracketClosingToken, CommaToken) {
			switch p.cur().Token {
			case CollateToken: // COLLATE NOCASE
				collateDecl, err := p.consumeToken(CollateToken)
				if err != nil {
					return nil, p.syntaxError()
				}
				newAttribute.Add(collateDecl)
				n, err := p.consumeToken(NocaseToken)
				if err != nil {
					return nil, p.syntaxError()
				}
				collateDecl.Add(n)
			default:
				// Unknown column constraint
				return nil, p.syntaxError()
			}
		}

		// The current token is either closing bracked or comma.

		// Closing bracket means table parsing stops.
		if tokens[p.index].Token == BracketClosingToken {
			p.index++
			break
		}

		// Comma means continue on next table column.
		p.index++
	}

	return indexDecl, nil
}

func (p *parser) parseTable(tokens []Token) (*Decl, error) {
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
	nameTable, err := p.parseTableName()
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

		switch p.cur().Token {
		case PrimaryToken:
			pkDecl, err := p.parsePrimaryKey()
			if err != nil {
				return nil, err
			}
			tableDecl.Add(pkDecl)
			continue
		default:
		}

		// Closing bracket ?
		if tokens[p.index].Token == BracketClosingToken {
			p.consumeToken(BracketClosingToken)
			break
		}

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

		// All the following tokens until bracket or comma are column constraints.
		// Column constraints can be listed in any order.
		for p.isNot(BracketClosingToken, CommaToken) {
			switch p.cur().Token {
			case UnsignedToken:
				p.consumeToken(UnsignedToken)
			case UniqueToken: // UNIQUE
				uniqueDecl, err := p.consumeToken(UniqueToken)
				if err != nil {
					return nil, err
				}
				newAttribute.Add(uniqueDecl)
			case NotToken: // NOT NULL
				if _, err = p.isNext(NullToken); err == nil {
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
			case PrimaryToken: // PRIMARY KEY
				if _, err = p.isNext(KeyToken); err == nil {
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
			case AutoincrementToken:
				autoincDecl, err := p.consumeToken(AutoincrementToken)
				if err != nil {
					return nil, err
				}
				newAttribute.Add(autoincDecl)
			case WithToken: // WITH TIME ZONE
				if strings.ToLower(newAttributeType.Lexeme) == "timestamp" {
					withDecl, err := p.consumeToken(WithToken)
					if err != nil {
						return nil, err
					}
					timeDecl, err := p.consumeToken(TimeToken)
					if err != nil {
						return nil, err
					}
					zoneDecl, err := p.consumeToken(ZoneToken)
					if err != nil {
						return nil, err
					}
					newAttributeType.Add(withDecl)
					withDecl.Add(timeDecl)
					timeDecl.Add(zoneDecl)
				}
			case DefaultToken: // DEFAULT
				dDecl, err := p.parseDefaultClause()
				if err != nil {
					return nil, err
				}
				newAttribute.Add(dDecl)
			default:
				// Unknown column constraint
				return nil, p.syntaxError()
			}
		}

		// The current token is either closing bracked or comma.

		// Closing bracket means table parsing stops.
		if tokens[p.index].Token == BracketClosingToken {
			p.index++
			break
		}

		// Comma means continue on next table column.
		p.index++
	}

	return tableDecl, nil
}

func (p *parser) parseDefaultClause() (*Decl, error) {
	dDecl, err := p.consumeToken(DefaultToken)
	if err != nil {
		return nil, err
	}

	var vDecl *Decl

	if p.is(SimpleQuoteToken) || p.is(DoubleQuoteToken) {
		vDecl, err = p.parseStringLiteral()
	} else {
		vDecl, err = p.consumeToken(NullToken, FloatToken, FalseToken, NumberToken, LocalTimestampToken, NowToken, ArgToken, NamedArgToken)
	}

	if err != nil {
		return nil, err
	}
	dDecl.Add(vDecl)
	return dDecl, nil
}

func (p *parser) parsePrimaryKey() (*Decl, error) {
	primaryDecl, err := p.consumeToken(PrimaryToken)
	if err != nil {
		return nil, err
	}

	keyDecl, err := p.consumeToken(KeyToken)
	if err != nil {
		return nil, err
	}
	primaryDecl.Add(keyDecl)

	_, err = p.consumeToken(BracketOpeningToken)
	if err != nil {
		return nil, err
	}

	for {
		d, err := p.parseQuotedToken()
		if err != nil {
			return nil, err
		}
		keyDecl.Add(d)

		d, err = p.consumeToken(CommaToken, BracketClosingToken)
		if err != nil {
			return nil, err
		}

		if d.Token == BracketClosingToken {
			break
		}
	}

	return primaryDecl, nil
}

func (p *parser) parseSchema(tokens []Token) (*Decl, error) {
	var err error
	schemaDecl := NewDecl(tokens[p.index])
	p.index++

	// Now we should found name
	name, err := p.parseAttribute()
	if err != nil {
		return nil, p.syntaxError()
	}
	schemaDecl.Add(name)

	return schemaDecl, nil
}
