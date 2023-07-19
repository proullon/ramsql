package executor

import (
	"fmt"
	"strings"
	"time"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/log"
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
	if decl.Decl[0].Token != parser.StringToken {
		return agnostic.Attribute{}, false, fmt.Errorf("engine: expected attribute type, got %v:%v", decl.Decl[0].Token, decl.Decl[0].Lexeme)
	}
	typeName = decl.Decl[0].Lexeme

	attr = agnostic.NewAttribute(name, typeName)

	// Maybe domain and special thing like primary key
	typeDecl := decl.Decl[1:]
	for i := range typeDecl {
		log.Debug("Got %v for %s %s", typeDecl[i], name, typeName)
		if typeDecl[i].Token == parser.AutoincrementToken {
			attr = attr.WithAutoIncrement()
		}

		if typeDecl[i].Token == parser.DefaultToken {
			//			log.Debug("we get a default value for %s: %s!\n", name, typeDecl[i].Decl[0].Lexeme)
			switch typeDecl[i].Decl[0].Token {
			case parser.LocalTimestampToken, parser.NowToken:
				log.Debug("Setting default value to NOW() func !\n")
				attr = attr.WithDefault(func() any { return time.Now() })
				//				attr.defaultValue = func() interface{} { return time.Now().Format(parser.DateLongFormat) }
			default:
				log.Debug("Setting default value to '%v'\n", typeDecl[i].Decl[0].Lexeme)
				//attr.defaultValue = typeDecl[i].Decl[0].Lexeme
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
