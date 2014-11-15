package engine

import (
	"errors"
	"io"
	"log"
	"net"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type executor func(*Engine, *parser.Decl) (string, error)

type Engine struct {
	ln           net.Listener
	tables       map[string]Table
	opsExecutors map[int]executor

	// Any value send to this channel (through Engine.stop)
	// Will stop the listening loop
	stop chan bool
}

func New() (e *Engine, err error) {
	initLog()

	e = &Engine{}

	e.stop = make(chan bool)

	e.opsExecutors = map[int]executor{
		parser.CreateToken: createExecutor,
		parser.TableToken:  createTableExecutor,
		parser.SelectToken: selectExecutor,
		parser.InsertToken: insertIntoTableExecutor,
	}

	e.tables = make(map[string]Table)

	err = e.start()
	if err != nil {
		return nil, err
	}

	return
}

func (e *Engine) Endpoint() net.Addr {
	return e.ln.Addr()
}

func (e *Engine) start() (err error) {
	e.ln, err = net.Listen("tcp", ":8080")

	if err != nil {
		log.Printf("Engine.listen: Cannot start listening: %s\n", err)
		return errors.New("Cannot start RamSQL server")
	}

	go e.listen()
	return nil
}

func (e *Engine) Stop() {
	e.stop <- true
}

func (e *Engine) listen() {
	newConnectionChannel := make(chan net.Conn)

	go func() {
		for {
			conn, err := e.ln.Accept()

			log.Printf("Engine.listen: accept")
			if err != nil {
				log.Printf("Engine.listen: Cannot accept new connection : %s", err)
				break
			}

			newConnectionChannel <- conn
		}
	}()

	for {
		select {
		case conn := <-newConnectionChannel:
			log.Printf("Engine.listen: new connection")
			go e.handleConnection(conn)
			break

		case <-e.stop:
			e.ln.Close()
			return
			break
		}
	}

}

func (e *Engine) handleConnection(conn net.Conn) {
	log.Printf("Engine.handleConnection")

	for {
		log.Printf("Engine.handleConnection: Reading")
		m, err := protocol.Read(conn)

		if err != nil && err != io.EOF {
			log.Printf("Enginge.handleConnection: cannot read : %s", err)
			conn.Close()
			return
		} else if err != nil {
			conn.Close()
			return
		}

		log.Printf("Engine.handleConnection: GOT <%s>", m.Value)

		instructions, err := parser.ParseInstruction(m.Value)
		if err != nil {
			protocol.Send(conn, protocol.Error, err.Error())
			continue
		}

		answer, err := e.executeQueries(instructions)
		if err != nil {
			protocol.Send(conn, protocol.Error, err.Error())
			continue
		}

		protocol.Send(conn, protocol.Result, answer)
	}
}

func (e *Engine) executeQueries(instructions []parser.Instruction) (string, error) {
	var completeAnswerString string

	for _, i := range instructions {
		answer, err := e.executeQuery(i)
		if err != nil {
			return "", err
		}
		completeAnswerString += answer
	}

	return completeAnswerString, nil
}

func (e *Engine) executeQuery(i parser.Instruction) (string, error) {
	log.Printf("Engine.executeQuery")
	i.PrettyPrint()

	if e.opsExecutors[i.Decls[0].Token] != nil {
		return e.opsExecutors[i.Decls[0].Token](e, i.Decls[0])
	}

	// switch i.Decls[0].Token {
	// // case parser.CreateToken:
	// // 	break
	// default:
	// 	return "", errors.New("Not Implemented")
	// 	break
	// }

	return "", errors.New("Not Implemented")
}

func createExecutor(e *Engine, createDecl *parser.Decl) (string, error) {
	log.Printf("createExecutor")

	if len(createDecl.Decl) == 0 {
		return "", errors.New("Parsing failed, no declaration after CREATE")
	}

	if e.opsExecutors[createDecl.Decl[0].Token] != nil {
		return e.opsExecutors[createDecl.Decl[0].Token](e, createDecl.Decl[0])
	}

	return "", errors.New("Parsing failed, unkown token " + createDecl.Decl[0].Lexeme)
}

func selectExecutor(e *Engine, createDecl *parser.Decl) (string, error) {
	log.Printf("selectExecutor")

	// For decl != FROM
	// get attribute to select

	// get FROM declaration

	// get WHERE declaration
	return "", errors.New("Parsing failed, unkown token " + createDecl.Decl[0].Lexeme)
}
