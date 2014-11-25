package ramsql

import (
	"fmt"

	"github.com/proullon/ramsql/engine/protocol"
)

type Result struct {
	err          error
	lastInsertId int64
	rowsAffected int64
}

// computeResult unmarshal raw data and create a Result
func computeResult(m *protocol.Message) (*Result, error) {
	r := &Result{}
	_, err := fmt.Sscanf(m.Value, "%d %d", &r.lastInsertId, &r.rowsAffected)
	return r, err
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.lastInsertId, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.rowsAffected, nil
}
