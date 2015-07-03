package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

/*
|-> update
	|-> account
	|-> set
	      |-> email
					|-> =
					|-> roger@gmail.com
  |-> where
        |-> id
					|-> =
					|-> 2
*/
func updateExecutor(e *Engine, updateDecl *parser.Decl, conn protocol.EngineConn) error {
	var num int64
	updateDecl.Stringy(0)

	// table name
	r := e.relation(updateDecl.Decl[0].Lexeme)
	if r == nil {
		return fmt.Errorf("Table %s does not exists", updateDecl.Decl[0].Lexeme)
	}

	// Set decl
	values, err := setExecutor(updateDecl.Decl[1])
	if err != nil {
		return err
	}

	// Where decl
	predicates, err := whereExecutor(updateDecl.Decl[2])
	if err != nil {
		return err
	}

	var ok bool
	for i := range r.rows {
		ok = true
		// If the row validate all predicates, write it
		for _, predicate := range predicates {
			if predicate.Evaluate(r.rows[i], r.table) == false {
				ok = false
				continue
			}
		}

		if ok {
			num++
			err = updateValues(r, i, values)
			if err != nil {
				return err
			}
		}
	}

	return conn.WriteResult(0, num)
}

/*
	|-> set
	      |-> email
					|-> =
					|-> roger@gmail.com
*/
func setExecutor(setDecl *parser.Decl) (map[string]interface{}, error) {

	values := make(map[string]interface{})

	for _, attr := range setDecl.Decl {
		values[attr.Lexeme] = attr.Decl[1].Lexeme
	}

	return values, nil
}

func updateValues(r *Relation, row int, values map[string]interface{}) error {

	for i := range r.table.attributes {
		val, ok := values[r.table.attributes[i].name]
		if !ok {
			continue
		}

		r.rows[row].Values[i] = fmt.Sprintf("%v", val)
	}

	return nil
}
