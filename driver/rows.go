package ramsql

import (
	"database/sql/driver"
	"errors"
	"io"

	"github.com/proullon/ramsql/engine/log"
)

type Rows struct {
	rowsChannel chan []string
	columns     []string
}

func newRows(channel chan []string) *Rows {
	log.Debug("newRows")
	r := &Rows{rowsChannel: channel}
	c := <-channel
	r.columns = c
	return r
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice.  If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator.
func (r *Rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// The dest slice may be populated only with
// a driver Value type, but excluding string.
// All string values must be converted to []byte.
//
// Next should return io.EOF when there are no more rows.
func (r *Rows) Next(dest []driver.Value) (err error) {

	value, ok := <-r.rowsChannel
	if !ok {
		return io.EOF
	}

	for i, v := range value {
		if len(dest) <= i {
			return fmt.Errorf("slice too short")
		}
		dest[i] = []byte(v)
	}

	return nil
}

func (r *Rows) setColumns(columns []string) {
	r.columns = columns
}

func assignValue(s string, v driver.Value) error {
	dest, ok := v.(*string)
	if !ok {
		err := errors.New("cannot assign value")
		return err
	}

	*dest = s
	return nil
}
