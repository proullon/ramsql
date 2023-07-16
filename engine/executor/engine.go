package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	//"fmt"
	//"io"

	"github.com/proullon/ramsql/engine/agnostic"
	//"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

// Engine is the root struct of RamSQL server
type Engine struct {
	memstore *agnostic.Engine
}

// New initialize a new RamSQL server
func NewEngine() (e *Engine, err error) {

	e = &Engine{
		memstore: agnostic.NewEngine(),
	}

	return
}

func (e *Engine) Begin() (*Tx, error) {
	tx, err := NewTx(context.Background(), e, sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (e *Engine) Stop() {
}

func createExecutor(t *Tx, decl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, ParsingError
	}

	if t.opsExecutors[decl.Decl[0].Token] != nil {
		return t.opsExecutors[decl.Decl[0].Token](t, decl.Decl[0])
	}

	return 0, 0, nil, NotImplemented
}

func grantExecutor(t *Tx, decl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {
	return 0, 1, nil, nil
}

func createTableExecutor(t *Tx, tableDecl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {
	var i int
	var schemaName string

	if len(tableDecl.Decl) == 0 {
		return 0, 0, nil, ParsingError
	}

	// Check for specific attribute
	for i < len(tableDecl.Decl) {
		if t.opsExecutors[tableDecl.Decl[i].Token] == nil {
			break
		}

		_, _, _, err := t.opsExecutors[tableDecl.Decl[i].Token](t, tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, err
		}
		i++
	}

	if d, ok := tableDecl.Has(parser.SchemaToken); ok {
		schemaName = d.Lexeme
	}

	relationName := tableDecl.Decl[i].Lexeme

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	exists := t.tx.CheckRelation(schemaName, relationName)
	if exists && ifNotExists {
		return 0, 0, nil, nil
	}
	if exists {
		return 0, 0, nil, errors.New("relation already exists")
	}

	var pk []string
	var attributes []agnostic.Attribute

	// Fetch attributes
	i++
	for i < len(tableDecl.Decl) {
		attr, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, err
		}
		attributes = append(attributes, attr)
		i++
	}

	err := t.tx.CreateRelation(schemaName, relationName, attributes, pk)
	if err != nil {
		return 0, 0, nil, err
	}
	return 0, 1, nil, nil

	/*



			// Check if table does not exists
			_, err := e.relation(schema, tableDecl.Decl[i].Lexeme)
			if err == nil && !ifNotExists {
				return fmt.Errorf("table %s already exists", tableDecl.Decl[i].Lexeme)
			}

			// Fetch table name
			t := NewTable(schema, tableDecl.Decl[i].Lexeme)

			// Fetch attributes
			i++
			for i < len(tableDecl.Decl) {
				attr, err := parseAttribute(tableDecl.Decl[i])
				if err != nil {
					return err
				}
				err = t.AddAttribute(attr)
				if err != nil {
					return err
				}

				i++
			}

			s, err := e.schema(schema)
			if err != nil {
				return err
			}
			s.add(t.name, NewRelation(t))
			conn.WriteResult(0, 1)
		return nil
	*/
}

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
func insertIntoTableExecutor(t *Tx, insertDecl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {

	var lastInsertedID int64
	var schemaName string
	var returningAttrs []string
	relationName := insertDecl.Decl[0].Decl[0].Lexeme

	// Check for RETURNING clause
	if len(insertDecl.Decl) > 2 {
		for i := range insertDecl.Decl {
			if insertDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := insertDecl.Decl[i]
				returningAttrs = append(returningAttrs, returningDecl.Decl[0].Lexeme)
			}
		}
	}

	var specifiedAttrs []string
	for _, d := range insertDecl.Decl[0].Decl[0].Decl {
		specifiedAttrs = append(specifiedAttrs, d.Lexeme)
	}

	var tuples []*agnostic.Tuple
	valuesDecl := insertDecl.Decl[1]
	for _, valueListDecl := range valuesDecl.Decl {
		values := getValues(specifiedAttrs, valueListDecl)
		tuple, err := t.tx.Insert(schemaName, relationName, values)
		if err != nil {
			return 0, 0, nil, err
		}
		tuples = append(tuples, tuple)

		// guess lastInsertedID
		if v := tuple.Values(); len(v) > 0 {
			if reflect.TypeOf(v[0]).ConvertibleTo(reflect.TypeOf(lastInsertedID)) {
				lastInsertedID = reflect.ValueOf(v[0]).Convert(reflect.TypeOf(lastInsertedID)).Int()
			}
		}
	}

	if len(returningAttrs) == 0 {
		return lastInsertedID, int64(len(tuples)), nil, nil
	}

	ch := make(chan *agnostic.Tuple, len(tuples)+2)
	collumns := &agnostic.Tuple{}
	for _, rattr := range returningAttrs {
		collumns.Append(rattr)
	}
	ch <- collumns

	for _, tuple := range tuples {
		returningTuple := &agnostic.Tuple{}
		for _, rattr := range returningAttrs {
			index, _, err := t.tx.RelationAttribute(schemaName, relationName, rattr)
			if err != nil {
				return 0, 0, nil, err
			}
			returningTuple.Append(tuple.Values()[index])
		}

		ch <- returningTuple
	}

	fmt.Printf("DEON\n")
	return lastInsertedID, int64(len(tuples)), ch, nil

	/*
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
	*/
}

func getValues(specifiedAttrs []string, valuesDecl *parser.Decl) map[string]any {
	values := make(map[string]any)

	for i, d := range valuesDecl.Decl {
		values[specifiedAttrs[i]] = d.Lexeme
	}

	return values
}

func hasIfNotExists(tableDecl *parser.Decl) bool {
	for _, d := range tableDecl.Decl {
		if d.Token == parser.IfToken {
			if len(d.Decl) > 0 && d.Decl[0].Token == parser.NotToken {
				not := d.Decl[0]
				if len(not.Decl) > 0 && not.Decl[0].Token == parser.ExistsToken {
					return true
				}
			}
		}
	}

	return false
}

/*
func lalalal() {
	autocommit := true
	for {
		stmt, err := conn.ReadStatement()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Warning("Enginge.handleConnection: cannot read : %s", err)
			return
		}

		instructions, err := parser.ParseInstruction(stmt)
		if err != nil {
			conn.WriteError(err)
			continue
		}

		if tx == nil {
			tx, err = e.Begin()
			if err != nil {
				conn.WriteError(err)
				continue
			}
		}

		err = e.executeQueries(tx, instructions, conn)
		if err != nil {
			conn.WriteError(err)
			continue
		}

		if autocommit {
			err = tx.Commit()
			if err != nil {
				conn.WriteError(err)
			}
			tx = nil
			continue
		}
	}
}

func (e *Engine) executeQueries(instructions []parser.Instruction, conn protocol.EngineConn) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fatal error: %s", r)
			return
		}
	}()

	for _, i := range instructions {
		err = e.executeQuery(i, conn)
		if err != nil {
			return err
		}
	}

	return nil
}
*/

//func (e *Engine) executeQuery(i parser.Instruction, conn protocol.EngineConn) error {
/*
	i.Decls[0].Stringy(0,
		func(format string, varargs ...any) {
			fmt.Printf(format, varargs...)
		})
*/
/*
	if e.opsExecutors[i.Decls[0].Token] != nil {
		return e.opsExecutors[i.Decls[0].Token](e, i.Decls[0], conn)
	}

	return errors.New("Not Implemented")
}
*/
