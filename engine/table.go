package engine

import (
	// "errors"
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
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
	// Check that name is not already taken
	// for i := range t.attributes {

	// }
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

func createTableExecutor(e *Engine, tableDecl *parser.Decl) (string, error) {

	// Fetch table name
	// if len(tableDecl.Decl) < 1 && tableDecl.Decl[0].Token != parser.StringToken {
	// 	return "", errors.New("No table name provided")
	// }
	t := NewTable(tableDecl.Decl[0].Lexeme)

	// Fetch attributes
	for i := 1; i < len(tableDecl.Decl); i++ {
		attr, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return "", err
		}
		err = t.AddAttribute(attr)
		if err != nil {
			return "", err
		}
	}

	e.relations[t.name] = NewRelation(t)
	return fmt.Sprintf("0 1"), nil
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
func insertIntoTableExecutor(e *Engine, insertDecl *parser.Decl) (string, error) {
	Info("insertIntoTableSelector")
	insertDecl.Stringy(0)

	// Get table and concerned attributes

	// Create a new tuple with values
	return "", NotImplemented
}
