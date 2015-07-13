package engine

import (
	"errors"
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

/*
|-> SELECT
    |-> *
    |-> FROM
        |-> account
    |-> WHERE
        |-> email
            |-> =
            |-> foo@bar.com
*/
func selectExecutor(e *Engine, selectDecl *parser.Decl, conn protocol.EngineConn) error {

	// get selected tables
	tables := fromExecutor(selectDecl.Decl[1])

	// get attribute to select
	attr, err := getSelectedAttributes(e, selectDecl.Decl[0], tables)
	if err != nil {
		return err
	}

	// Instanciate a new select functor
	functors, err := getSelectFunctors(selectDecl)

	// get WHERE declaration
	predicates, err := whereExecutor(selectDecl.Decl[2])
	if err != nil {
		return err
	}

	// Mybe order by ?
	// TODO: implement ORDER BY

	// and select
	err = selectRows(e, attr, tables, conn, predicates, functors)
	if err != nil {
		return err
	}

	return nil
}

type selectFunctor interface {
	Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error
	Feed(t *Tuple) error
	Done() error
}

// getSelectFunctors instanciate new functors for COUNT, MAX, MIN, AVG, ... and default select functor that return rows to client
// If a functor is specified, no attribute can be selected ?
func getSelectFunctors(attr *parser.Decl) ([]selectFunctor, error) {
	var functors []selectFunctor

	for i := range attr.Decl {

		if attr.Decl[i].Token == parser.FromToken {
			break
		}

		if attr.Decl[i].Token == parser.CountToken {
			f := &countSelectFunction{}
			functors = append(functors, f)
		}
	}

	if len(functors) == 0 {
		f := &defaultSelectFunction{}
		functors = append(functors, f)
	}

	return functors, nil

}

type defaultSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
}

func (f *defaultSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias

	return f.conn.WriteRowHeader(f.alias)
}

func (f *defaultSelectFunction) Feed(t *Tuple) error {
	var row []string
	for _, value := range t.Values {
		row = append(row, fmt.Sprintf("%v", value))
	}
	return f.conn.WriteRow(row)
}

func (f *defaultSelectFunction) Done() error {
	return f.conn.WriteRowEnd()
}

type countSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
	Count      int64
}

func (f *countSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias
	return nil
}

func (f *countSelectFunction) Feed(t *Tuple) error {
	f.Count++
	return nil
}

func (f *countSelectFunction) Done() error {
	err := f.conn.WriteRowHeader(f.alias)
	if err != nil {
		return err
	}

	err = f.conn.WriteRow([]string{fmt.Sprintf("%d", f.Count)})
	if err != nil {
		return err
	}

	return f.conn.WriteRowEnd()
}

/*
   |-> WHERE
       |-> email
           |-> =
           |-> foo@bar.com
*/
func whereExecutor(whereDecl *parser.Decl) ([]Predicate, error) {
	var predicates []Predicate

	for i := range whereDecl.Decl {
		var p Predicate

		// 1 PREDICATE
		if whereDecl.Decl[i].Lexeme == "1" {
			predicates = append(predicates, TruePredicate)
			continue
		}

		p.LeftValue.lexeme = whereDecl.Decl[i].Lexeme
		if len(whereDecl.Decl[i].Decl) < 2 {
			return nil, fmt.Errorf("Malformed predicate \"%s\"", whereDecl.Decl[i].Lexeme)
		}

		op, err := NewOperator(whereDecl.Decl[i].Decl[0].Token, whereDecl.Decl[i].Decl[0].Lexeme)
		if err != nil {
			return nil, err
		}
		p.Operator = op

		p.RightValue.lexeme = whereDecl.Decl[i].Decl[1].Lexeme
		p.RightValue.valid = true

		predicates = append(predicates, p)
	}

	if len(predicates) == 0 {
		return nil, fmt.Errorf("No predicates provided")
	}

	return predicates, nil
}

/*
|-> FROM
    |-> account
*/
func fromExecutor(fromDecl *parser.Decl) []*Table {
	var tables []*Table
	for _, t := range fromDecl.Decl {
		tables = append(tables, NewTable(t.Lexeme))
	}

	return tables
}

func getSelectedAttributes(e *Engine, attr *parser.Decl, tables []*Table) ([]Attribute, error) {
	var attributes []Attribute

	// handle *
	if attr.Token == parser.StarToken {
		for _, table := range tables {
			r := e.relation(table.name)
			if r == nil {
				return nil, errors.New("Relation " + table.name + " not found")
			}

			attributes = append(attributes, r.table.attributes...)
		}
	}

	// handle COUNT
	if attr.Token == parser.CountToken {
		attributes = append(attributes, NewAttribute("COUNT", "int", false))
	}

	return attributes, nil
}

func selectRows(e *Engine, attr []Attribute, tables []*Table, conn protocol.EngineConn, predicates []Predicate, functors []selectFunctor) error {

	// get relations and write lock them for reading
	var relations []*Relation
	for _, t := range tables {
		r := e.relation(t.name)
		r.RLock()
		defer r.RUnlock()
		relations = append(relations, r)
	}

	// Write header
	var header []string
	for _, a := range attr {
		header = append(header, a.name)
	}

	// Initialize functors here
	for i := range functors {
		if err := functors[i].Init(e, conn, header, header); err != nil {
			return err
		}
	}

	// Perform actual check of predicates on every row
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
			for i := range functors {
				err := functors[i].Feed(tuple)
				if err != nil {
					return err
				}
			}
		}
	}

	for i := range functors {
		err := functors[i].Done()
		if err != nil {
			return err
		}
	}

	return nil
}

func defaultSelectOperation() {
}

func writeRow(conn protocol.EngineConn, t *Tuple) error {
	var row []string
	for _, value := range t.Values {
		row = append(row, fmt.Sprintf("%s", value))
	}
	return conn.WriteRow(row)
}
