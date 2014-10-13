package ramsql

type Tx struct {
}

func (t *Tx) Commit() error {
	return newError(NotImplemented)
}

func (t *Tx) Rollback() error {
	return newError(NotImplemented)
}
