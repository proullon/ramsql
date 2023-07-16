package executor

import (
	"context"
	"database/sql"
	//"errors"
	//"fmt"
	//"io"

	"github.com/proullon/ramsql/engine/agnostic"
	//"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

type executorFunc func(*Engine, *parser.Decl) error

// Engine is the root struct of RamSQL server
type Engine struct {
	opsExecutors map[int]executorFunc

	memstore *agnostic.Engine
}

// New initialize a new RamSQL server
func NewEngine() (e *Engine, err error) {

	e = &Engine{
		memstore: agnostic.NewEngine(),
	}

	e.opsExecutors = map[int]executorFunc{
		//		parser.CreateToken:   createExecutor,
		//		parser.TableToken:    createTableExecutor,
		//		parser.SchemaToken:   createSchemaExecutor,
		//		parser.IndexToken:    createIndexExecutor,
		//		parser.SelectToken:   selectExecutor,
		//		parser.InsertToken:   insertIntoTableExecutor,
		//		parser.DeleteToken:   deleteExecutor,
		//		parser.UpdateToken:   updateExecutor,
		//		parser.IfToken:       ifExecutor,
		//		parser.NotToken:      notExecutor,
		//		parser.ExistsToken:   existsExecutor,
		//		parser.TruncateToken: truncateExecutor,
		//		parser.DropToken:     dropExecutor,
		//		parser.GrantToken:    grantExecutor,
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
