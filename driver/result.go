package ramsql

type Result struct {
	lastInsertId int64
	rowsAffected int64
}

// computeResult unmarshal raw data and create a Result
func computeResult(data []byte) *Result {
	return &Result{}
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	return 0, newError(NotImplemented)
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	return 0, newError(NotImplemented)
}
