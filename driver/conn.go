package ramsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/proullon/ramsql/engine/executor"
)

// Conn implements sql/driver Conn interface
//
// All Conn implementations should implement the following interaces: Pinger, SessionResetter, and Validator.
//
// If a Conn does not implement QueryerContext, the sql package's DB.Query will fall back to Queryer;
// if the Conn does not implement Queryer either, DB.Query will first prepare a query, execute the statement, and then close the statement.
//
// If named parameters or context are supported, the driver's Conn should implement: ExecerContext, QueryerContext, ConnPrepareContext, and ConnBeginTx.
//
// The returned connection is only used by one goroutine at a time.
//
// https://pkg.go.dev/database/sql/driver#Conn
//
// https://pkg.go.dev/database/sql/driver#Pinger
// https://pkg.go.dev/database/sql/driver#SessionResetter
// https://pkg.go.dev/database/sql/driver#Validator
// https://pkg.go.dev/database/sql/driver#QueryerContext
// https://pkg.go.dev/database/sql/driver#ExecerContext
// https://pkg.go.dev/database/sql/driver#ConnPrepareContext
// https://pkg.go.dev/database/sql/driver#ConnBeginTx
type Conn struct {
	e *executor.Engine
}

func newConn(e *executor.Engine) *Conn {
	return &Conn{e: e}
}

// Ping
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove the Conn from pool.
//
// Implemented for Pinger interface
func (c *Conn) Ping(ctx context.Context) error {
	return nil
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before. If the driver returns ErrBadConn
// the connection is discarded.
//
// Implemented for SessionResetter interface
func (c *Conn) ResetSession(ctx context.Context) error {
	return nil
}

// IsValid is called prior to placing the connection into the
// connection pool. The connection will be discarded if false is returned.
//
// Implemented for Validator interface
func (c *Conn) IsValid() bool {
	return true
}

// Prepare returns a prepared statement, bound to this connection.
//
// Implemented for Conn interface
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
//
// Implemented for Conn interface
func (c *Conn) Close() error {
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
//
// Implemented for Conn interface
func (c *Conn) Begin() (driver.Tx, error) {
	return executor.NewTx(context.Background(), c.e, sql.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	o := sql.TxOptions{
		Isolation: sql.IsolationLevel(opts.Isolation),
		ReadOnly:  opts.ReadOnly,
	}
	return executor.NewTx(ctx, c.e, o)
}

// QueryContext is the sql package prefered way to run QUERY.
//
// Implemented for QueryerContext interface
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {

	tx, err := c.e.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	a := make([]executor.NamedValue, len(args))
	for i, arg := range args {
		a[i].Name = arg.Name
		a[i].Ordinal = arg.Ordinal
		a[i].Value = arg.Value
	}

	cols, ch, err := tx.QueryContext(ctx, query, a)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	fmt.Printf("QUERYCONTEXXT CALLED for %s\n", query)
	return newRows(cols, ch), nil
}

// ExecContext is the sql package prefered way to run Exec
//
// Implemented for ExecerContext interface
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	tx, err := c.e.Begin()
	if err != nil {
		return nil, err
	}

	a := make([]executor.NamedValue, len(args))
	for i, arg := range args {
		a[i].Name = arg.Name
		a[i].Ordinal = arg.Ordinal
		a[i].Value = arg.Value
	}

	r := &Result{}
	r.lastInsertedID, r.rowsAffected, r.err = tx.ExecContext(ctx, query, a)

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return r, r.err
}
