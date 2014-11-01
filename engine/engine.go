package engine

import (
	"errors"
	"io"
	"log"
	"net"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type executor func(*Engine, parser.Instruction) (string, error)

var opsExecutors = map[int]executor{
	parser.CreateToken: createExecutor,
}

type Engine struct {
	ln     net.Listener
	tables map[string]Table
}

func New() (e *Engine, err error) {
	initLog()

	e = &Engine{}
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

func (e *Engine) listen() {

	for {
		log.Printf("Engine.listen: accept")
		conn, err := e.ln.Accept()
		if err != nil {
			log.Printf("Engine.listen: Cannot accept new connection : %s", err)
			continue
		}

		log.Printf("Engine.listen: new connection")
		go e.handleConnection(conn)
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
	log.Printf("Engine.executeQuery: %v", i)

	if opsExecutors[i.Decls[0].Token] != nil {
		opsExecutors[i.Decls[0].Token](e, i)
	}

	switch i.Decls[0].Token {
	// case parser.CreateToken:
	// 	break
	default:
		return "", errors.New("Not Implemented")
		break
	}

	return "", errors.New("Not Implemented")
}

func createExecutor(e *Engine, i parser.Instruction) (string, error) {
	log.Printf("createExecutor")
	return "", nil
}
