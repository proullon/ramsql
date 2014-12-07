package engine

import (
	"errors"
	"fmt"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type Table struct {
	name       string
	attributes []Attribute
}

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

func (t *Table) Insert(values []interface{}) error {
	return nil
}

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

	t := NewTable(tableDecl.Decl[0].Lexeme)

	// Fetch attributes
	for i := 1; i < len(tableDecl.Decl); i++ {
		attr, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return err
		}
		err = t.AddAttribute(attr)
		if err != nil {
			return err
		}
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
	log.Info("insertIntoTableSelector")

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
	var assigned bool = false

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
func selectExecutor(e *Engine, createDecl *parser.Decl, conn protocol.EngineConn) error {
	log.Info("selectExecutor")

	// get selected tables
	tables := getSelectedTables(createDecl.Decl[1])
	log.Info("Selected tables are %v", tables)

	// get attribute to select
	attr, err := getSelectedAttributes(e, createDecl.Decl[0], tables)
	if err != nil {
		return err
	}
	log.Info("Selected attributes are %v", attr)

	// get WHERE declaration

	// and select
	err = selectRows(e, attr, tables, conn)
	if err != nil {
		return err
	}

	return nil
}

/*
|-> FROM
    |-> account
*/
func getSelectedTables(fromDecl *parser.Decl) []*Table {
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

	return attributes, nil
}

func selectRows(e *Engine, attr []Attribute, tables []*Table, conn protocol.EngineConn) error {
	log.Info("selecting rows")

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
	log.Critical("Writing row header %v", header)
	err := conn.WriteRowHeader(header)
	if err != nil {
		return err
	}

	// I don't have a fucking clue now
	for _, tuple := range relations[0].rows {
		var row []string
		for _, value := range tuple.Values {
			row = append(row, fmt.Sprintf("%s", value))
		}

		log.Critical("Writing row  %v", row)
		err := conn.WriteRow(row)
		if err != nil {
			return err
		}
	}

	return conn.WriteRowEnd()
}
