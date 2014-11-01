package engine

import (
	"errors"
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
)

type Field struct {
	Name string
	Type interface{}
}

type Table struct {
	Name   string
	Fields []Field
}

func createTableExecutor(e *Engine, i parser.Instruction) (string, error) {

	if len(i.Decls) < 3 {
		return "", errors.New("No table name provided")
	}

	t := Table{
		Name: i.Decls[2].Lexeme,
	}

	e.tables[t.Name] = t
	return fmt.Sprintf("table %s created", t.Name), nil
}
