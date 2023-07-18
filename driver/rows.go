package ramsql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/executor"
)

// Rows implements the sql/driver Rows interface
type Rows struct {
	columns []string
	tuples  []*agnostic.Tuple
	tx      *executor.Tx
	idx     int
	end     int
}

func newRows(cols []string, tuples []*agnostic.Tuple) *Rows {

	r := &Rows{
		tuples:  tuples,
		columns: cols,
		end:     len(tuples) - 1,
	}

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

	/*
		if r.rowsChannel == nil {
			return nil
		}

		_, ok := <-r.rowsChannel
		if !ok {
			return nil
		}

		// Tels UnlimitedRowsChannel to close itself
		//r.rowsChannel <- []string{}
		r.rowsChannel = nil
	*/
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
	if r.idx > r.end {
		return io.EOF
	}

	tuple := r.tuples[r.idx]
	r.idx++

	values := tuple.Values()
	if len(dest) < len(values) {
		return fmt.Errorf("slice too short (%d slots for %d values)", len(dest), len(values))
	}

	for i, v := range values {
		dest[i] = v
	}

	return nil
	/*
		value, ok := <-r.rowsChannel
		if !ok {
			r.rowsChannel = nil
			return io.EOF
		}

		if len(dest) < len(value) {
			return fmt.Errorf("slice too short (%d slots for %d values)", len(dest), len(value))
		}

		for i, v := range value {
			if v == "<nil>" {
				dest[i] = nil
				continue
			}

			switch v.(type) {
			case string:
				val, _ := v.(string)
				// TODO: make rowsChannel send virtualRows,
				// so we have the type and don't blindy try to parse date here
				if t, err := parser.ParseDate(val); err == nil {
					dest[i] = *t
				} else {
					dest[i] = []byte(val)
				}
			default:
				dest[i] = v
			}

		}

		return nil
	*/
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
