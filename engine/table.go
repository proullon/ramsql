package engine

import (
	// "errors"
	"fmt"
	"log"

	"github.com/proullon/ramsql/engine/parser"
)

type Attribute struct {
	Name     string
	TypeName string
	Type     interface{}
}

type Row struct {
	Values []interface{}
}

type Table struct {
	Name       string
	Attributes []Attribute
	Rows       []Row
}

func (t Table) String() string {
	stringy := t.Name + " ("
	for i, a := range t.Attributes {
		if i != 0 {
			stringy += " | "
		}
		stringy += a.Name + " " + a.TypeName
	}
	stringy += ")"
	return stringy
}

func (t *Table) Insert(values []interface{}) error {
	return nil
}

func createTableExecutor(e *Engine, tableDecl *parser.Decl) (string, error) {

	// Fetch table name
	// if len(tableDecl.Decl) < 1 && tableDecl.Decl[0].Token != parser.StringToken {
	// 	return "", errors.New("No table name provided")
	// }
	t := Table{
		Name: tableDecl.Decl[0].Lexeme,
	}

	// Fetch attributes
	for i := 1; i < len(tableDecl.Decl); i++ {
		attr := Attribute{}

		attr.Name = tableDecl.Decl[i].Lexeme
		attr.TypeName = tableDecl.Decl[i].Decl[0].Lexeme

		t.Attributes = append(t.Attributes, attr)
	}

	e.tables[t.Name] = t
	fmt.Println(t)
	return fmt.Sprintf("0 1"), nil
}

func insertIntoTableExecutor(e *Engine, insertDecl *parser.Decl) (string, error) {
	log.Printf("insertIntoTableSelector")

	return " ", nil
}
