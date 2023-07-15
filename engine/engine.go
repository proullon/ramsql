package engine

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type executor func(*Engine, *parser.Decl, protocol.EngineConn) error

// Engine is the root struct of RamSQL server
type Engine struct {
	endpoint     protocol.EngineEndpoint
	opsExecutors map[int]executor

	// Any value send to this channel (through Engine.stop)
	// Will stop the listening loop
	stop chan bool

	agnostic.Engine
}

// New initialize a new RamSQL server
func New(endpoint protocol.EngineEndpoint) (e *Engine, err error) {

	e = &Engine{
		endpoint: endpoint,
		agnostic.New(),
	}

	e.stop = make(chan bool)

	e.opsExecutors = map[int]executor{
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

	err = e.Start()
	if err != nil {
		return nil, err
	}

	return
}

func (e *Engine) Start() (err error) {
	go e.listen()
	return nil
}

// Stop shutdown the RamSQL server
func (e *Engine) Stop() {
	if e.stop == nil {
		// already stopped
		return
	}
	go func() {
		e.stop <- true
		close(e.stop)
		e.stop = nil
	}()
}

/*
func (e *Engine) Commit(t *Transaction) error {
	_, err := t.Commit()
	return err
}

func (e *Engine) Rollback(t *Transaction) {
	t.Rollback()
}
*/

func (e *Engine) relation(schema, name string) (*Relation, error) {
	if schema == "" {
		schema = "public"
	}

	s, err := e.schema(schema)
	if err != nil {
		return nil, err
	}

	return s.Relation(name)
}

func (e *Engine) dropRelation(schema, name string) {
	if schema == "" {
		schema = "public"
	}

	e.Lock()
	e.schemas[schema].Remove(name)
	e.Unlock()
}

func (e *Engine) addSchema(s *Schema) {
	e.Lock()
	defer e.Unlock()
	e.schemas[s.name] = s
}

func (e *Engine) dropSchema(name string) bool {
	e.Lock()
	defer e.Unlock()
	if _, ok := e.schemas[name]; !ok {
		return false
	}
	delete(e.schemas, name)
	return true
}

func (e *Engine) listen() {
	newConnectionChannel := make(chan protocol.EngineConn)

	go func() {
		for {
			conn, err := e.endpoint.Accept()
			if err != nil {
				e.Stop()
				return
			}

			newConnectionChannel <- conn
		}
	}()

	for {
		select {
		case conn := <-newConnectionChannel:
			go e.handleConnection(conn)
			break

		case <-e.stop:
			e.endpoint.Close()
			return
		}
	}

}

func (e *Engine) handleConnection(conn protocol.EngineConn) {

	for {
		stmt, err := conn.ReadStatement()
		if err == io.EOF {
			// Todo: close engine if there is no conn left
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

		err = e.executeQueries(instructions, conn)
		if err != nil {
			conn.WriteError(err)
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

func (e *Engine) executeQuery(i parser.Instruction, conn protocol.EngineConn) error {
	/*
		i.Decls[0].Stringy(0,
			func(format string, varargs ...any) {
				fmt.Printf(format, varargs...)
			})
	*/

	if e.opsExecutors[i.Decls[0].Token] != nil {
		return e.opsExecutors[i.Decls[0].Token](e, i.Decls[0], conn)
	}

	return errors.New("Not Implemented")
}

func createExecutor(e *Engine, createDecl *parser.Decl, conn protocol.EngineConn) error {

	if len(createDecl.Decl) == 0 {
		return errors.New("Parsing failed, no declaration after CREATE")
	}

	if e.opsExecutors[createDecl.Decl[0].Token] != nil {
		return e.opsExecutors[createDecl.Decl[0].Token](e, createDecl.Decl[0], conn)
	}

	return errors.New("Parsing failed, unknown token " + createDecl.Decl[0].Lexeme)
}

func grantExecutor(e *Engine, decl *parser.Decl, conn protocol.EngineConn) error {
	return conn.WriteResult(0, 0)
}
