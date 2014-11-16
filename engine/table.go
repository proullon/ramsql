package engine

import (
	// "errors"
	"fmt"
	"log"

	"github.com/proullon/ramsql/engine/parser"
)

// Domain is the set of allowable values for an Attribute.
type Domain struct {
}

// Attribute is a named column of a relation
// AKA Field
// AKA Column
type Attribute struct {
	name         string
	typeName     string
	typeInstance interface{}
	defaultValue interface{}
}

// Tuple is a row in a relation
type Tuple struct {
	Values []interface{}
}

// Relation is a table with column and rows
// AKA File
type Relation struct {
	table Table
	rows  []Tuple
}

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

func (t *Table) AddAttribute(attr Attribute) error {
	// Check that name is not already taken
	// for i := range t.attributes {

	// }
	// t.attributes = append(t.attributes, attr)
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

	// // Fetch table name
	// // if len(tableDecl.Decl) < 1 && tableDecl.Decl[0].Token != parser.StringToken {
	// // 	return "", errors.New("No table name provided")
	// // }
	// t := Table{
	// 	Name: tableDecl.Decl[0].Lexeme,
	// }

	// // Fetch attributes
	// for i := 1; i < len(tableDecl.Decl); i++ {
	// 	attr := Attribute{}

	// 	attr.Name = tableDecl.Decl[i].Lexeme
	// 	attr.TypeName = tableDecl.Decl[i].Decl[0].Lexeme

	// 	t.Attributes = append(t.Attributes, attr)
	// }

	// e.tables[t.Name] = t
	// fmt.Println(t)
	return fmt.Sprintf("0 1"), nil
}

func insertIntoTableExecutor(e *Engine, insertDecl *parser.Decl) (string, error) {
	log.Printf("insertIntoTableSelector")

	return " ", nil
}
