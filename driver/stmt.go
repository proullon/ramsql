package ramsql

import (
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/proullon/ramsql/engine/log"
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

	stmt.conn.mutex.Lock()
	return stmt
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *Stmt) Close() error {
	return fmt.Errorf("Not implemented.")
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
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	defer s.conn.mutex.Unlock()
	var finalQuery string

	// replace $* by arguments in query string
	finalQuery = replaceArguments(s.query, args)
	log.Info("Exec <%s>\n", finalQuery)

	// Send query to server
	err := s.conn.conn.WriteExec(finalQuery)
	if err != nil {
		log.Warning("Exec: Cannot send query to server: %s", err)
		return nil, fmt.Errorf("Cannot send query to server: %s", err)
	}

	// Get answer from server
	lastInsertedID, rowsAffected, err := s.conn.conn.ReadResult()
	if err != nil {
		return nil, err
	}

	// Create a driver.Result
	return newResult(lastInsertedID, rowsAffected), nil
}

// Query executes a query that may return rows, such as a
// SELECT.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	defer s.conn.mutex.Unlock()

	finalQuery := replaceArguments(s.query, args)
	log.Info("Query <%s>\n", finalQuery)
	err := s.conn.conn.WriteQuery(finalQuery)
	if err != nil {
		return nil, err
	}

	rowsChannel, err := s.conn.conn.ReadRows()
	if err != nil {
		return nil, err
	}

	r := newRows(rowsChannel)
	return r, nil
}

// replace $* by arguments in query string
func replaceArguments(query string, args []driver.Value) string {

	if strings.Count(query, "?") == len(args) {
		return replaceArgumentsODBC(query, args)
	}

	for argumentIndex := 1; ; argumentIndex++ {
		queryParts := strings.Split(query, fmt.Sprintf("$%d", argumentIndex))
		if len(queryParts) == 1 {
			return query
		}

		query = ""
		for i, queryPart := range queryParts {
			query += queryPart
			if i != len(queryParts)-1 {
				// Test if Value is a string, if so, add simple quotes
				_, ok := args[argumentIndex-1].(string)
				if ok && !strings.HasSuffix(query, "'") {
					query += `'` + strings.Replace(fmt.Sprintf("%s", args[argumentIndex-1]), `"`, `""`, -1) + `'`
				} else if ok {
					query += fmt.Sprintf("%s", args[argumentIndex-1])
				} else {
					query += fmt.Sprintf("%v", args[argumentIndex-1])
				}
			}
		}
	}

}

func replaceArgumentsODBC(query string, args []driver.Value) string {
	var finalQuery string

	queryParts := strings.Split(query, "?")
	finalQuery = queryParts[0]
	for i := range args {
		arg := fmt.Sprintf("%v", args[i])
		if strings.Count(arg, " ") > 0 {
			arg = "'" + arg + "'"
		}
		finalQuery += arg
		finalQuery += queryParts[i+1]
	}

	return finalQuery
}
