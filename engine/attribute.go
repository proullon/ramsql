package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
)

// Domain is the set of allowable values for an Attribute.
type Domain struct {
}

// Attribute is a named column of a relation
// AKA Field
// AKA Column
type Attribute struct {
	name         string
	typeName     string
	typeInstance interface{}
	defaultValue interface{}
	domain       Domain
}

func parseAttribute(decl *parser.Decl) (Attribute, error) {
	attr := Attribute{}

	// Attribute name
	if decl.Token != parser.StringToken {
		return attr, fmt.Errorf("engine: expected attribute name, got %v", decl.Token)
	}
	attr.name = decl.Lexeme

	// Attribute type
	if len(decl.Decl) < 1 || decl.Decl[0].Token != parser.StringToken {
		return attr, fmt.Errorf("engine: expected attribute type, got %v:%v", decl.Token, decl.Lexeme)
	}
	attr.typeName = decl.Decl[0].Lexeme

	// Maybe domain and special thing like primary key
	return attr, nil
}

func NewAttribute(name string, typeName string) Attribute {
	a := Attribute{
		name:     name,
		typeName: typeName,
	}

	return a
}
