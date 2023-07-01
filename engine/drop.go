package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

func dropExecutor(e *Engine, dropDecl *parser.Decl, conn protocol.EngineConn) error {

	if _, ok := dropDecl.Has(parser.TableToken); ok {
		return dropTable(e, dropDecl, conn)
	}
	if _, ok := dropDecl.Has(parser.SchemaToken); ok {
		return dropSchema(e, dropDecl, conn)
	}

	return fmt.Errorf("DROP not implemented")
}

func dropTable(e *Engine, dropDecl *parser.Decl, conn protocol.EngineConn) error {
	// Should have table token
	if dropDecl.Decl == nil ||
		len(dropDecl.Decl) != 1 ||
		dropDecl.Decl[0].Token != parser.TableToken ||
		len(dropDecl.Decl[0].Decl) != 1 {
		return fmt.Errorf("unexpected drop arguments")
	}

	tableDecl := dropDecl.Decl[0].Decl[0]
	schema := ""
	if d, ok := tableDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}

	r := e.relation(schema, tableDecl.Lexeme)
	if r == nil {
		return fmt.Errorf("relation '%s' not found", tableDecl.Lexeme)
	}

	e.dropRelation(schema, tableDecl.Lexeme)

	return conn.WriteResult(0, 1)
}

func dropSchema(e *Engine, dropDecl *parser.Decl, conn protocol.EngineConn) error {
	// Should have schema token
	if dropDecl.Decl == nil ||
		len(dropDecl.Decl) != 1 ||
		dropDecl.Decl[0].Token != parser.SchemaToken ||
		len(dropDecl.Decl[0].Decl) != 1 {
		return fmt.Errorf("unexpected drop arguments")
	}

	schema := dropDecl.Decl[0].Decl[0].Lexeme

	if e.dropSchema(schema) == false {
		return fmt.Errorf("schema '%s' not found", schema)
	}

	return conn.WriteResult(0, 1)
}
