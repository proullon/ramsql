package ramsql

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("ramsql", &RamSQLDriver{})
}

type RamSQLDriver struct {
}

func (rs *RamSQLDriver) Open(name string) (driver.Conn, error) {

	return &Conn{}, nil
}
