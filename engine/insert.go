package engine

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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
	    |-> (
	        |-> Roullon
	        |-> Pierre
	        |-> pierre.roullon@gmail.com
	|-> RETURNING
	        |-> email
*/
func insertIntoTableExecutor(e *Engine, insertDecl *parser.Decl, conn protocol.EngineConn) error {
	// Get table and concerned attributes and write lock it
	intoDecl := insertDecl.Decl[0]
	r, attributes, err := getRelation(e, intoDecl)
	if err != nil {
		return err
	}
	r.Lock()
	defer r.Unlock()

	// Check for RETURNING clause
	var returnedAttribute string
	if len(insertDecl.Decl) > 2 {
		for i := range insertDecl.Decl {
			if insertDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := insertDecl.Decl[i]
				returnedAttribute = returningDecl.Decl[0].Lexeme
				break
			}
		}
	}

	// Create a new tuple with values
	var tuples []Tuple
	valuesDecl := insertDecl.Decl[1]
	for _, valueListDecl := range valuesDecl.Decl {
		// TODO handle all inserts atomically
		t, err := insert(r, attributes, valueListDecl.Decl, returnedAttribute)
		if err != nil {
			return err
		}
		tuples = append(tuples, *t)
	}

	// if RETURNING decl is present
	if returnedAttribute != "" {
		conn.WriteRowHeader([]string{returnedAttribute})
		for _, t := range tuples {
			val, err := r.Get(returnedAttribute, t)
			if err != nil {
				continue
			}
			conn.WriteRow([]any{val})
		}
		conn.WriteRowEnd()
		return nil
	}

	if len(tuples) == 0 {
		conn.WriteResult(0, 0)
		return nil
	}

	value, err := r.Get("id", tuples[len(tuples)-1])
	if err != nil {
		conn.WriteResult(0, (int64)(len(tuples)))
		return nil
	}

	if val, ok := value.(int64); ok {
		conn.WriteResult(val, (int64)(len(tuples)))
		return nil
	}

	conn.WriteResult(0, (int64)(len(tuples)))
	return nil
}

/*
|-> insert (18)

	|-> into (30)
	    |-> bar (68)
	        |-> foo (29)
	        |-> baz (68)
	|-> values (31)
	    |-> ( (3)
	        |-> yep (68)
*/
func getRelation(e *Engine, intoDecl *parser.Decl) (*Relation, []*parser.Decl, error) {
	var schema string

	// Decl[0] is the table name
	table := intoDecl.Decl[0]

	if d, ok := table.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
		// remove Schema declaration from table declaration to allow buggy insert() matching to work
		table.Decl = table.Decl[1:]
	}

	r, err := e.relation(schema, table.Lexeme)
	if err != nil {
		return nil, nil, err
	}

	for i := range intoDecl.Decl[0].Decl {
		err := attributeExistsInTable(e, intoDecl.Decl[0].Decl[i].Lexeme, schema, intoDecl.Decl[0].Lexeme)
		if err != nil {
			return nil, nil, err
		}
	}

	return r, intoDecl.Decl[0].Decl, nil
}

func insert(r *Relation, attributes []*parser.Decl, values []*parser.Decl, returnedAttribute string) (*Tuple, error) {
	var assigned = false
	var id int64
	var valuesindex int

	// Create tuple
	t := NewTuple()

	for attrindex, attr := range r.table.attributes {
		assigned = false

		for x, decl := range attributes {
			if attr.name == decl.Lexeme && attr.autoIncrement == false && strings.ToLower(values[x].Lexeme) == "null" {
				valuesindex = x
				assigned = true
				t.Append(nil)
				continue
			}

			if attr.name == decl.Lexeme && attr.autoIncrement == false {
				// Before adding value in tuple, check it's not a builtin func or arithmetic operation
				switch values[x].Token {
				case parser.NowToken:
					t.Append(time.Now().Format(parser.DateLongFormat))
				default:
					switch strings.ToLower(attr.typeName) {
					case "int64", "int":
						val, err := strconv.ParseInt(values[x].Lexeme, 10, 64)
						if err != nil {
							return nil, err
						}
						t.Append(val)
					case "numeric", "decimal":
						val, err := strconv.ParseFloat(values[x].Lexeme, 64)
						if err != nil {
							return nil, err
						}
						t.Append(val)
					default:
						t.Append(values[x].Lexeme)
					}
				}
				valuesindex = x
				assigned = true
			}
		}

		// If attribute is AUTO INCREMENT, compute it and assign it
		if attr.autoIncrement {
			assigned = true
			id = int64(len(r.rows) + 1)
			t.Append(id)
		}

		// Do we have a UNIQUE attribute ? if so
		if attr.unique {
			for i := range r.rows { // check all value already in relation (yup, no index tree)
				if r.rows[i].Values[attrindex].(string) == string(values[valuesindex].Lexeme) {
					return nil, fmt.Errorf("UNIQUE constraint violation")
				}
			}
		}

		// If values was not explicitly given, set default value
		if assigned == false {
			switch val := attr.defaultValue.(type) {
			case func() interface{}:
				v := (func() interface{})(val)()
				log.Debug("Setting func value '%v' to %s\n", v, attr.name)
				t.Append(v)
			default:
				log.Debug("Setting default value '%v' to %s\n", val, attr.name)
				t.Append(attr.defaultValue)
			}
		}
	}

	log.Info("New tuple : %v", t)

	// Insert tuple
	err := r.Insert(t)
	if err != nil {
		return nil, err
	}

	return t, nil
}
