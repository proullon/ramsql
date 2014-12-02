package ramsql

type Result struct {
	err            error
	lastInsertedId int64
	rowsAffected   int64
}

func newResult(lastInsertedId int64, rowsAffected int64) *Result {
	r := &Result{
		lastInsertedId: lastInsertedId,
		rowsAffected:   rowsAffected,
	}

	return r
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.lastInsertedId, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.rowsAffected, nil
}
