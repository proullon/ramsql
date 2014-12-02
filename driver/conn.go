package ramsql

import (
	"database/sql/driver"
	"log"
	"sync"

	"github.com/proullon/ramsql/engine/protocol"
)

type Conn struct {
	// Mutex is locked when a Statement is created
	// then released on Statement.Exec or Statement.Query
	mutex sync.Mutex

	// Socket is the network connection to RamSQL engine
	conn protocol.DriverConn
	// socket net.Conn
}

func NewConn(conn protocol.DriverConn) driver.Conn {
	return &Conn{conn: conn}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	log.Printf("Conn.Prepare: Got <%s>\n", query)

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
	log.Printf("Conn.Close")
	c.conn.Close()
	return nil
}

// Begin starts and returns a new transaction.
func (c *Conn) Begin() (driver.Tx, error) {
	log.Printf("Conn.Begin")
	return &Tx{}, newError(NotImplemented)
}
