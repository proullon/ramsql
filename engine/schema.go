package engine

import (
// "fmt"
// "sync"
// "github.com/proullon/ramsql/engine/parser"
// "github.com/proullon/ramsql/engine/protocol"
)

/*
func createSchemaExecutor(e *Engine, schemaDecl *parser.Decl, conn protocol.EngineConn) error {
	var name string

	if len(schemaDecl.Decl) == 0 {
		return fmt.Errorf("parsing failed, malformed query")
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(schemaDecl)

	if d, ok := schemaDecl.Has(parser.StringToken); ok {
		name = d.Lexeme
	}

	// Check if schema does not exists
	_, err := e.schema(name)
	if err == nil && !ifNotExists {
		return fmt.Errorf("schema %s already exists", name)
	}

	e.addSchema(NewSchema(name))

	conn.WriteResult(0, 1)
	return nil
}
*/
