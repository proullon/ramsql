package engine

import (
	"container/list"
)

type Change struct {
	current *Tuple
	old     Tuple
}

type Transaction struct {
	locks []Relation

	// list of Change
	changes *list.List

	err error
}

func NewTransaction() (Transaction, error) {
	t := Transaction{
		changes: list.New(),
	}

	return t, nil
}

func (t Transaction) Commit() error {
	t.unlock()
	return t.Error()
}

func (t Transaction) Rollback() {
	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		c := b.Value.(Change)
		for i := range c.current.values {
			c.current.values[i] = c.old.values[i]
		}
		t.changes.Remove(b)
	}

	t.unlock()
}

func (t Transaction) Error() error {
	return t.err
}

func (t Transaction) unlock() {

	// Remove links to be GC'd faster
	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		t.changes.Remove(b)
	}

	// Unlock all touched tables
	for _, r := range t.locks {
		r.Unlock()
	}
}
