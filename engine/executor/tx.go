package executor

import (
	"context"
	"database/sql"
	"errors"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/parser"
)

type executorFunc func(*Tx, *parser.Decl) (int64, int64, chan *agnostic.Tuple, error)

var (
	NotImplemented = errors.New("not implemented")
	ParsingError   = errors.New("parsing error")
)

type NamedValue struct {
	Name    string
	Ordinal int
	Value   any
}

type Tx struct {
	e            *Engine
	tx           *agnostic.Transaction
	opsExecutors map[int]executorFunc
}

func NewTx(ctx context.Context, e *Engine, opts sql.TxOptions) (*Tx, error) {
	tx, err := e.memstore.Begin()
	if err != nil {
		return nil, err
	}

	t := &Tx{
		e:  e,
		tx: tx,
	}

	t.opsExecutors = map[int]executorFunc{
		parser.CreateToken: createExecutor,
		parser.TableToken:  createTableExecutor,
		//		parser.SchemaToken:   createSchemaExecutor,
		//		parser.IndexToken:    createIndexExecutor,
		//		parser.SelectToken:   selectExecutor,
		parser.InsertToken: insertIntoTableExecutor,
		//		parser.DeleteToken:   deleteExecutor,
		//		parser.UpdateToken:   updateExecutor,
		//		parser.IfToken:       ifExecutor,
		//		parser.NotToken:      notExecutor,
		//		parser.ExistsToken:   existsExecutor,
		//		parser.TruncateToken: truncateExecutor,
		//		parser.DropToken:     dropExecutor,
		parser.GrantToken: grantExecutor,
	}
	return t, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args []NamedValue) (chan *agnostic.Tuple, error) {
	return nil, NotImplemented
}

// Commit the transaction on server
func (t *Tx) Commit() error {
	_, err := t.tx.Commit()
	return err
}

// Rollback all changes
func (t *Tx) Rollback() error {
	t.tx.Rollback()
	return nil
}

func (t *Tx) ExecContext(ctx context.Context, query string, args []NamedValue) (int64, int64, error) {

	instructions, err := parser.ParseInstruction(query)
	if err != nil {
		return 0, 0, err
	}

	var lastInsertedID, rowsAffected, aff int64
	for _, instruct := range instructions {
		lastInsertedID, aff, err = t.executeQuery(instruct)
		if err != nil {
			return 0, 0, err
		}
		rowsAffected += aff
	}

	return lastInsertedID, rowsAffected, nil
}

func (t *Tx) executeQuery(i parser.Instruction) (int64, int64, error) {

	/*
		i.Decls[0].Stringy(0,
		func(format string, varargs ...any) {
			fmt.Printf(format, varargs...)
		})
	*/

	if t.opsExecutors[i.Decls[0].Token] == nil {
		return 0, 0, NotImplemented
	}

	l, r, _, err := t.opsExecutors[i.Decls[0].Token](t, i.Decls[0])
	if err != nil {
		return 0, 0, err
	}

	return l, r, nil
}
