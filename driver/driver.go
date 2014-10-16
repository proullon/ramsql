package ramsql

import (
	"database/sql"
	"database/sql/driver"
	"log"

	"github.com/proullon/ramsql/engine"
)

func init() {
	sql.Register("ramsql", &RamSQLDriver{})
}

type RamSQLDriver struct {
	// pool is the pool of active connection to server
	pool []driver.Conn

	// server is the engine instance started by driver
	server *engine.Engine
}

// Open return an active connection so RamSQL server
// If there is no connection in pool, start a new server.
func (rs *RamSQLDriver) Open(name string) (conn driver.Conn, err error) {
	log.Printf("RamSQLDriver.Open")

	if rs.server == nil {
		if rs.server, err = engine.New(); err != nil {
			return
		}
	}

	conn, err = connectToRamSQLServer("tcp", rs.server.Endpoint().String())
	if err != nil {
		return nil, err
	}

	rs.pool = append(rs.pool, conn)
	return conn, nil
}
