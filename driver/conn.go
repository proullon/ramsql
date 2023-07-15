package ramsql

import (
	"database/sql/driver"
)

// Conn implements sql/driver Conn interface
//
// All Conn implementations should implement the following interaces: Pinger, SessionResetter, and Validator.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query will fall back to Queryer;
// if the Conn does not implement Queryer either, DB.Query will first prepare a query, execute the statement, and then close the statement.
//
// The returned connection is only used by one goroutine at a time.
//
// https://pkg.go.dev/database/sql/driver#Pinger
// https://pkg.go.dev/database/sql/driver#SessionResetter
// https://pkg.go.dev/database/sql/driver#Validator
// https://pkg.go.dev/database/sql/driver#QueryerContext
type Conn struct {
	// this conn belongs to this server
	parent *Server
}

func newConn(parent *Server) driver.Conn {
	parent.openingConn()
	return &Conn{parent: parent}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {

	stmt := prepareStatement(c, query)

	return stmt, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
func (c *Conn) Close() error {
	if c.parent != nil {
		c.parent.closingConn()
	}

	return nil
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {

	tx := Tx{
		conn: c,
	}

	return &tx, nil
}
