package engine

import (
	// "errors"
	// "fmt"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

func deleteExecutor(e *Engine, deleteDecl *parser.Decl, conn protocol.EngineConn) error {
	log.Info("deleteExecutor")

	// get tables to be deleted
	tables := fromExecutor(deleteDecl.Decl[0])
	log.Info("deleted tables are %v", tables)

	// If len is 1, it means no predicates so truncate table
	if len(deleteDecl.Decl) == 1 {
		return truncateTable(e, tables[0], conn)
	}

	// get WHERE declaration
	predicates, err := whereExecutor(deleteDecl.Decl[1])
	if err != nil {
		return err
	}

	// and delete
	return deleteRows(e, tables, conn, predicates)
}

func deleteRows(e *Engine, tables []*Table, conn protocol.EngineConn, predicates []Predicate) error {
	var rowsDeleted int64

	// get relations and write lock them
	var relations []*Relation
	for _, t := range tables {
		r := e.relation(t.name)
		// r.writeLock ?
		relations = append(relations, r)
	}

	var ok bool
	for _, tuple := range relations[0].rows {
		ok = true
		// If the row validate all predicates, write it
		for _, predicate := range predicates {
			if predicate.Evaluate(tuple, relations[0].table) == false {
				ok = false
				continue
			}
		}

		if ok {
			log.Critical("DELETE THIS SHIT %v", tuple)
		}
	}

	return conn.WriteResult(0, rowsDeleted)
}

func truncateTable(e *Engine, table *Table, conn protocol.EngineConn) error {
	var rowsDeleted int64

	// get relations and write lock them
	r := e.relation(table.name)
	rowsDeleted = int64(len(r.rows))
	r.rows = make([]*Tuple, 0)

	return conn.WriteResult(0, rowsDeleted)
}
