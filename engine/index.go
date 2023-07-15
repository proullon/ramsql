package engine

import (
//"fmt"
//"unsafe"

// "github.com/proullon/ramsql/engine/parser"
// "github.com/proullon/ramsql/engine/protocol"
)

/*
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

	var test uintptr
	var t2 *Table

	_ = t2

	test = 203948
	t2 = (*Table)(unsafe.Pointer(test))

	_ = schema
	_ = relation
	_ = newIndex
	conn.WriteResult(0, 1)
	return nil
}
*/
