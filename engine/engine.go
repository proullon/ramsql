package engine

import (
	"errors"
	"log"
	"net"
)

type Engine struct {
	ln net.Listener
}

func New() (e *Engine, err error) {
	initLog()

	e = &Engine{}
	err = e.listen()
	if err != nil {
		return nil, err
	}

	return
}

func (e *Engine) Endpoint() net.Addr {
	return e.ln.Addr()
}

func (e *Engine) listen() (err error) {
	e.ln, err = net.Listen("tcp", ":8080")

	if err != nil {
		log.Printf("Engine.listen: Cannot start listening: %s\n", err)
		return errors.New("Cannot start RamSQL server")
	}

	go func() {
		for {
			conn, err := e.ln.Accept()
			if err != nil {
				// handle error
				continue
			}
			go e.handleConnection(conn)
		}
	}()

	return nil
}

func (e *Engine) handleConnection(conn net.Conn) {
	log.Printf("Engine.handleConnection")
}
