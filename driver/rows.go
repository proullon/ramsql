package ramsql

import (
	"database/sql/driver"
	"fmt"
	"io"

	"github.com/proullon/ramsql/engine/agnostic"
)

// Rows implements the sql/driver Rows interface
type Rows struct {
	columns []string
	tuples  []*agnostic.Tuple
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
}
