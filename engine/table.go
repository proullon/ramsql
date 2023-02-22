package engine

import (
	"fmt"

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

	// Check for specific attribute
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

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	// Check if table does not exists
	r := e.relation(tableDecl.Decl[i].Lexeme)
	if r != nil && !ifNotExists {
		return fmt.Errorf("table %s already exists", tableDecl.Decl[i].Lexeme)
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
