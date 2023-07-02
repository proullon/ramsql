package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type Index interface {
}

type BTreeIndex struct {
}

type HashIndex struct {
}

func NewIndex() *Index {
	return nil
}

func createIndexExecutor(e *Engine, indexDecl *parser.Decl, conn protocol.EngineConn) error {
	var i int
	var schema, relation, index string

	indexDecl.Stringy(0, nil)

	if len(indexDecl.Decl) == 0 {
		return fmt.Errorf("parsing failed, malformed query")
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(indexDecl)

	fmt.Printf("IfNotExists: %v\n", ifNotExists)
	if ifNotExists {
		i++
	}

	// Fetch index name
	index = indexDecl.Decl[i].Lexeme
	i++

	_ = index
	newIndex := NewIndex()

	fmt.Printf("Index name: '%s'\n", index)

	if d, ok := indexDecl.Has(parser.TableToken); ok {
		relation = d.Lexeme
		i++
	}

	_ = schema
	_ = relation
	_ = newIndex
	/*
		var attrs []Attribute
		// Fetch attributes
		for i < len(indexDecl.Decl) {
			attr, err := parseAttribute(indexDecl.Decl[i])
			if err != nil {
				return err
			}
			attrs = append(attrs, attr)

			i++
		}

		r, err := e.relation(schema, relation)
		if err != nil {
			return err
		}

		_ = newIndex
		_ = r
	*/
	conn.WriteResult(0, 1)
	return nil
}
