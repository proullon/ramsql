package engine

import (
	"errors"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type executor func(*Engine, *parser.Decl, protocol.EngineConn) error

type Engine struct {
	endpoint     protocol.EngineEndpoint
	relations    map[string]*Relation
	opsExecutors map[int]executor

	// Any value send to this channel (through Engine.stop)
	// Will stop the listening loop
	stop chan bool
}

func New(endpoint protocol.EngineEndpoint) (e *Engine, err error) {

	e = &Engine{
		endpoint: endpoint,
	}

	e.stop = make(chan bool)

	e.opsExecutors = map[int]executor{
		parser.CreateToken: createExecutor,
		parser.TableToken:  createTableExecutor,
		parser.SelectToken: selectExecutor,
		parser.InsertToken: insertIntoTableExecutor,
	}

	e.relations = make(map[string]*Relation)

	err = e.start()
	if err != nil {
		return nil, err
	}

	return
}

func (e *Engine) start() (err error) {
	go e.listen()
	return nil
}

func (e *Engine) Stop() {
	e.stop <- true
}

func (e *Engine) relation(name string) *Relation {
	// Lock ?
	r := e.relations[name]
	// Unlock ?

	return r
}

func (e *Engine) listen() {
	newConnectionChannel := make(chan protocol.EngineConn)

	go func() {
		for {
			conn, err := e.endpoint.Accept()

			log.Info("Engine.listen: accept")
			if err != nil {
				log.Warning("Engine.listen: Cannot accept new connection : %s", err)
				break
			}

			newConnectionChannel <- conn
		}
	}()

	for {
		select {
		case conn := <-newConnectionChannel:
			log.Info("Engine.listen: new connection")
			go e.handleConnection(conn)
			break

		case <-e.stop:
			e.endpoint.Close()
			return
		}
	}

}

func (e *Engine) handleConnection(conn protocol.EngineConn) {
	log.Info("Engine.handleConnection")

	for {
		log.Info("Engine.handleConnection: Reading")
		stmt, err := conn.ReadStatement()
		if err != nil {
			log.Warning("Enginge.handleConnection: cannot read : %s", err)
			conn.WriteError(err)
			return
		}

		log.Notice("Engine.handleConnection: GOT <%s>", stmt)

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

func (e *Engine) executeQueries(instructions []parser.Instruction, conn protocol.EngineConn) error {
	for _, i := range instructions {
		err := e.executeQuery(i, conn)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) executeQuery(i parser.Instruction, conn protocol.EngineConn) error {
	log.Info("Engine.executeQuery")

	if e.opsExecutors[i.Decls[0].Token] != nil {
		return e.opsExecutors[i.Decls[0].Token](e, i.Decls[0], conn)
	}

	return errors.New("Not Implemented")
}

func createExecutor(e *Engine, createDecl *parser.Decl, conn protocol.EngineConn) error {
	log.Info("createExecutor")

	if len(createDecl.Decl) == 0 {
		return errors.New("Parsing failed, no declaration after CREATE")
	}

	if e.opsExecutors[createDecl.Decl[0].Token] != nil {
		return e.opsExecutors[createDecl.Decl[0].Token](e, createDecl.Decl[0], conn)
	}

	return errors.New("Parsing failed, unkown token " + createDecl.Decl[0].Lexeme)
}

func selectExecutor(e *Engine, createDecl *parser.Decl, conn protocol.EngineConn) error {
	Info("selectExecutor")

	// For decl != FROM
	// get attribute to select

	// get FROM declaration

	// get WHERE declaration
	return errors.New("Parsing failed, unkown token " + createDecl.Decl[0].Lexeme)
}
