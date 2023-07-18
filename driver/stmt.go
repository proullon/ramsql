package ramsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

// Stmt implements the Statement interface of sql/driver
type Stmt struct {
	conn     *Conn
	query    string
	numInput int
}

func countArguments(query string) int {
	for id := 1; id > 0; id++ {
		sep := fmt.Sprintf("$%d", id)
		if strings.Count(query, sep) == 0 {
			return id - 1
		}
	}

	return -1
}

func prepareStatement(c *Conn, query string) *Stmt {

	// Parse number of arguments here
	// Should handler either Postgres ($*) or ODBC (?) parameter markers
	numInput := strings.Count(query, "?")
	// if numInput == 0, maybe it's Postgres format
	if numInput == 0 {
		numInput = countArguments(query)
	}

	// Create statement
	stmt := &Stmt{
		conn:     c,
		query:    query,
		numInput: numInput,
	}

	return stmt
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() error {
	return nil
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
	return s.numInput
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
func (s *Stmt) Exec(args []driver.Value) (r driver.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fatalf error: %s", r)
			return
		}
	}()

	if s.query == "" {
		return nil, fmt.Errorf("empty statement")
	}

	var cargs []driver.NamedValue
	for i, arg := range args {
		cargs = append(cargs, driver.NamedValue{Name: fmt.Sprintf("%d", i+1), Ordinal: i + 1, Value: arg})
	}

	return s.conn.ExecContext(context.Background(), s.query, cargs)
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) Query(args []driver.Value) (r driver.Rows, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fatalf error: %s", r)
			return
		}
	}()

	if s.query == "" {
		return nil, fmt.Errorf("empty statement")
	}
	var cargs []driver.NamedValue
	for i, arg := range args {
		cargs = append(cargs, driver.NamedValue{Name: fmt.Sprintf("%d", i+1), Ordinal: i + 1, Value: arg})
	}

	return s.conn.QueryContext(context.Background(), s.query, cargs)
}
