package engine

import (
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

func truncateExecutor(e *Engine, trDecl *parser.Decl, conn protocol.EngineConn) error {
	var schema string

	if d, ok := trDecl.Decl[0].Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}

	// get tables to be deleted
	table := NewTable(schema, trDecl.Decl[0].Lexeme)

	return truncateTable(e, table, conn)
}

func truncateTable(e *Engine, table *Table, conn protocol.EngineConn) error {
	var rowsDeleted int64

	// get relations and write lock them
	r, err := e.relation(table.schema, table.name)
	if err != nil {
		return err
	}
	r.Lock()
	defer r.Unlock()

	if r.rows != nil {
		rowsDeleted = int64(len(r.rows))
	}
	r.rows = make([]*Tuple, 0)

	return conn.WriteResult(0, rowsDeleted)
}
