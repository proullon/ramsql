package ramsql

import (
	"database/sql/driver"
	"fmt"
	"log"
)

type Stmt struct {
	conn  *Conn
	query string
}

func prepareStatement(c *Conn, query string) *Stmt {
	log.Printf("prepareStatement: query <%v>", query)

	stmt := &Stmt{
		conn:  c,
		query: query,
	}

	stmt.conn.mutex.Lock()
	return stmt
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() error {
	log.Printf("Stmt.Close")

	return newError(NotImplemented)
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	log.Printf("Stmt.NumInput")

	return 0
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	log.Printf("Stmt.Exec: %v", args)
	defer s.conn.mutex.Unlock()
	var finalQuery string

	// replace $* by arguments in query string
	for i, arg := range args {
		log.Printf("Stmt.Exec: Arg %d : %v", i, arg)
	}

	// Send query to server
	n, err := s.conn.socket.Write([]byte(finalQuery))
	if err != nil {
		log.Printf("Stmt.Exec: %s", err)
		return nil, fmt.Errorf("Cannot send query to server: %s", err)
	}

	if n != len(finalQuery) {
		return nil, fmt.Errorf("Cannot send entire query to server")
	}

	// Wait for engine answer
	var answer []byte
	n, err = s.conn.socket.Read(answer)
	if err != nil {
		log.Printf("Stmt.Exec: %s", err)
		return nil, fmt.Errorf("Cannot read server answer")
	}

	// Create a driver.Result
	return computeResult(answer), nil
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	log.Printf("Stmt.Query")
	defer s.conn.mutex.Unlock()

	return nil, newError(NotImplemented)
}
