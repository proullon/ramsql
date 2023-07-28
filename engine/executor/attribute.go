package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/parser"
)

func parseAttribute(decl *parser.Decl) (attr agnostic.Attribute, isPk bool, err error) {
	var name, typeName string

	// Attribute name
	if decl.Token != parser.StringToken {
		return agnostic.Attribute{}, false, fmt.Errorf("engine: expected attribute name, got %v", decl.Token)
	}
	name = strings.ToLower(decl.Lexeme)

	// Attribute type
	if len(decl.Decl) < 1 {
		return attr, false, fmt.Errorf("Attribute %s has no type", decl.Lexeme)
	}
	switch decl.Decl[0].Token {
	case parser.DecimalToken:
		typeName = "float"
	case parser.NumberToken:
		typeName = "int"
	case parser.DateToken:
		typeName = "date"
	case parser.StringToken:
		typeName = decl.Decl[0].Lexeme
	default:
		return agnostic.Attribute{}, false, fmt.Errorf("engine: expected attribute type, got %v:%v", decl.Decl[0].Token, decl.Decl[0].Lexeme)
	}

	attr = agnostic.NewAttribute(name, typeName)

	// Maybe domain and special thing like primary key
	typeDecl := decl.Decl[1:]
	for i := range typeDecl {
		if typeDecl[i].Token == parser.AutoincrementToken {
			attr = attr.WithAutoIncrement()
		}

		if typeDecl[i].Token == parser.DefaultToken {
			switch typeDecl[i].Decl[0].Token {
			case parser.LocalTimestampToken, parser.NowToken:
				attr = attr.WithDefault(func() any { return time.Now() })
			default:
				v, err := agnostic.ToInstance(typeDecl[i].Decl[0].Lexeme, typeName)
				if err != nil {
					return agnostic.Attribute{}, false, err
				}
				attr = attr.WithDefaultConst(v)
			}
		}

		// Check if attribute is unique
		if typeDecl[i].Token == parser.UniqueToken {
			attr = attr.WithUnique()
		}
		if typeDecl[i].Token == parser.PrimaryToken {
			if len(typeDecl[i].Decl) > 0 && typeDecl[i].Decl[0].Token == parser.KeyToken {
				isPk = true
			}
		}

	}

	if strings.ToLower(typeName) == "bigserial" {
		attr = attr.WithAutoIncrement()
	}

	return attr, isPk, nil
}
