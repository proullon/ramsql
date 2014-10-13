package ramsql

import (
	"database/sql/driver"
	"log"
)

type Conn struct {
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	log.Printf("Conn.Prepare: Got <%s>\n", query)
	return &Stmt{}, newError(NotImplemented)
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
	return newError(NotImplemented)
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	return &Tx{}, newError(NotImplemented)
}
