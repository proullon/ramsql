package engine

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
)

type Engine struct {
	ln net.Listener
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
		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
		buffer = buffer[:len(buffer)-1]

		if err != nil && err != io.EOF {
			log.Printf("Enginge.handleConnection: cannot read : %s", err)
			conn.Close()
		}

		log.Printf("Engine.handleConnection: GOT <%s>", buffer)

		answer := parse(string(buffer))
		fmt.Fprint(conn, "%s\n", answer)
	}
}
