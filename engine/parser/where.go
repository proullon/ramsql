package parser

import (
	"fmt"
)

func (p *parser) parseWhere(selectDecl *Decl) error {

	// May be WHERE  here
	// Can be ORDER BY if WHERE cause if implicit
	whereDecl, err := p.consumeToken(WhereToken)
	if err != nil {
		return err
	}
	selectDecl.Add(whereDecl)

	// Now should be a list of: Attribute and Operator and Value
	gotClause := false
	for {
		if !p.hasNext() && gotClause {
			break
		}

		if p.is(OrderToken, LimitToken, ForToken) {
			break
		}

		attributeDecl, err := p.parseCondition()
		if err != nil {
			return err
		}
		whereDecl.Add(attributeDecl)

		if p.is(AndToken, OrToken) {
			linkDecl, err := p.consumeToken(p.cur().Token)
			if err != nil {
				return err
			}
			whereDecl.Add(linkDecl)
		}

		// Got at least one clause
		gotClause = true
	}

	return nil
}

func (p *parser) parseCondition() (*Decl, error) {
	// Optionnaly, brackets

	// We may have the WHERE 1 condition
	if t := p.cur(); t.Token == NumberToken && t.Lexeme == "1" {
		attributeDecl := NewDecl(t)
		p.next()
		// in case of 1 = 1
		if p.cur().Token == EqualityToken {
			t, err := p.isNext(NumberToken)
			if err == nil && t.Lexeme == "1" {
				p.consumeToken(EqualityToken)
				p.consumeToken(NumberToken)
			}
		}
		return attributeDecl, nil
	}

	// do we have brackets ?
	hasBracket := false
	if p.is(BracketOpeningToken) {
		p.consumeToken(BracketOpeningToken)
		hasBracket = true
	}

	// Attribute
	attributeDecl, err := p.parseAttribute()
	if err != nil {
		return nil, err
	}

	switch p.cur().Token {
	case EqualityToken, DistinctnessToken, LeftDipleToken, RightDipleToken, LessOrEqualToken, GreaterOrEqualToken:
		decl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
		break
	case InToken:
		inDecl, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(inDecl)
		return attributeDecl, nil
	case NotToken:
		notDecl, err := p.consumeToken(p.cur().Token)
		if err != nil {
			return nil, err
		}

		if p.cur().Token != InToken {
			return nil, fmt.Errorf("expected IN after NOT")
		}

		inDecl, err := p.parseIn()
		if err != nil {
			return nil, err
		}
		notDecl.Add(inDecl)

		attributeDecl.Add(notDecl)
		return attributeDecl, nil
	case IsToken:
		decl, err := p.consumeToken(IsToken)
		if err != nil {
			return nil, err
		}
		attributeDecl.Add(decl)
		if p.cur().Token == NotToken {
			notDecl, err := p.consumeToken(NotToken)
			if err != nil {
				return nil, err
			}
			decl.Add(notDecl)
		}
		if p.cur().Token == NullToken {
			nullDecl, err := p.consumeToken(NullToken)
			if err != nil {
				return nil, err
			}
			decl.Add(nullDecl)
		}
		return attributeDecl, nil
	}

	// Value
	valueDecl, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	attributeDecl.Add(valueDecl)

	if hasBracket {
		if _, err = p.consumeToken(BracketClosingToken); err != nil {
			return nil, err
		}
	}

	return attributeDecl, nil
}
