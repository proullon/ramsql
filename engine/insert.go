package engine

import (
	"errors"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

/*
|-> INSERT
    |-> INTO
        |-> user
            |-> last_name
            |-> first_name
            |-> email
    |-> VALUES
        |-> Roullon
        |-> Pierre
        |-> pierre.roullon@gmail.com
*/
func insertIntoTableExecutor(e *Engine, insertDecl *parser.Decl, conn protocol.EngineConn) error {

	// Get table and concerned attributes and write lock it
	r, attributes, err := getRelation(e, insertDecl.Decl[0])
	if err != nil {
		return err
	}
	r.Lock()
	defer r.Unlock()

	// Create a new tuple with values
	id, err := insert(r, attributes, insertDecl.Decl[1].Decl)
	if err != nil {
		return err
	}

	conn.WriteResult(id, 1)
	return nil
}

/*
|-> INTO
    |-> user
        |-> last_name
        |-> first_name
        |-> email
*/
func getRelation(e *Engine, intoDecl *parser.Decl) (*Relation, []*parser.Decl, error) {

	// Decl[0] is the table name
	r := e.relation(intoDecl.Decl[0].Lexeme)
	if r == nil {
		return nil, nil, errors.New("table " + intoDecl.Decl[0].Lexeme + " does not exists")
	}

	return r, intoDecl.Decl[0].Decl, nil
}

func insert(r *Relation, attributes []*parser.Decl, values []*parser.Decl) (int64, error) {
	var assigned = false
	var id int64

	// Create tuple
	t := NewTuple()
	for _, attr := range r.table.attributes {
		assigned = false

		for x, decl := range attributes {

			if attr.name == decl.Lexeme && attr.autoIncrement == false {
				t.Append(values[x].Lexeme)
				assigned = true
			}
		}

		// If attribute is AUTO INCREMENT, compute it and assign it
		if attr.autoIncrement {
			assigned = true
			id = int64(len(r.rows) + 1)
			t.Append(id)
		}
		// If values was not explictly given, set default value
		if assigned == false {
			t.Append(attr.defaultValue)
		}
	}

	log.Info("New tuple : %v", t)

	// Insert tuple
	err := r.Insert(t)
	if err != nil {
		return 0, err
	}

	return id, nil
}
