package engine

import (
	"errors"
	"fmt"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

// Table is defined by a name and attributes
// A table with data is called a Relation
type Table struct {
	name       string
	attributes []Attribute
}

// NewTable initializes a new Table
func NewTable(name string) *Table {
	t := &Table{
		name: name,
	}

	return t
}

// AddAttribute is used by CREATE TABLE and ALTER TABLE
// Want to check that name isn't already taken
func (t *Table) AddAttribute(attr Attribute) error {
	t.attributes = append(t.attributes, attr)
	return nil
}

// String returns a printable string with table name and attributes
func (t Table) String() string {
	stringy := t.name + " ("
	for i, a := range t.attributes {
		if i != 0 {
			stringy += " | "
		}
		stringy += a.name + " " + a.typeName
	}
	stringy += ")"
	return stringy
}

func createTableExecutor(e *Engine, tableDecl *parser.Decl, conn protocol.EngineConn) error {
	var i int

	if len(tableDecl.Decl) == 0 {
		return fmt.Errorf("parsing failed, malformed query")
	}

	// Fetch constraint
	i = 0
	for i < len(tableDecl.Decl) {

		if e.opsExecutors[tableDecl.Decl[i].Token] != nil {
			if err := e.opsExecutors[tableDecl.Decl[i].Token](e, tableDecl.Decl[i], conn); err != nil {
				return err
			}
		} else {
			break
		}

		i++
	}

	// Fetch table name
	t := NewTable(tableDecl.Decl[i].Lexeme)

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

	e.relations[t.name] = NewRelation(t)
	conn.WriteResult(0, 1)
	return nil
}

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

	// Get table and concerned attributes
	r, attributes, err := getRelation(e, insertDecl.Decl[0])
	if err != nil {
		return err
	}

	// Create a new tuple with values
	err = insert(r, attributes, insertDecl.Decl[1].Decl)
	if err != nil {
		return err
	}

	conn.WriteResult(0, 1)
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

func insert(r *Relation, attributes []*parser.Decl, values []*parser.Decl) error {
	var assigned = false

	// Create tuple
	t := NewTuple()
	for _, attr := range r.table.attributes {
		assigned = false
		for x, decl := range attributes {
			if attr.name == decl.Lexeme {
				t.Append(values[x].Lexeme)
				assigned = true
			}
		}

		// If values was not explictly given, set default value
		if assigned == false {
			t.Append(attr.defaultValue)
		}
	}

	log.Critical("New tuple : %v", t)

	// Insert tuple
	err := r.Insert(t)
	if err != nil {
		return err
	}

	return nil
}

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

	// and select
	err = selectRows(e, attr, tables, conn, predicates, functors)
	if err != nil {
		return err
	}

	return nil
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
		attributes = append(attributes, NewAttribute("COUNT", "int"))
	}

	return attributes, nil
}

func selectRows(e *Engine, attr []Attribute, tables []*Table, conn protocol.EngineConn, predicates []Predicate, functors []selectFunctor) error {
	log.Debug("selecting rows")

	// get relations and write lock them
	var relations []*Relation
	for _, t := range tables {
		r := e.relation(t.name)
		// r.writeLock ?
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

	// I don't have a fucking clue now
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
			//err = writeRow(conn, tuple)
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
