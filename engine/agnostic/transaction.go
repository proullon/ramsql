package agnostic

import (
	"container/list"
	"fmt"
)

type Transaction struct {
	e     *Engine
	locks []Relation

	// list of Change
	changes *list.List

	err error
}

func NewTransaction(e *Engine) (*Transaction, error) {
	t := Transaction{
		e:       e,
		changes: list.New(),
	}

	return &t, nil
}

func (t Transaction) Commit() (int, error) {
	if err := t.aborted(); err != nil {
		return 0, err
	}

	changed := t.changes.Len()

	// Remove links to be GC'd faster
	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		t.changes.Remove(b)
	}

	t.unlock()
	return changed, t.Error()
}

func (t Transaction) Rollback() {
	if err := t.aborted(); err != nil {
		return
	}

	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		switch b.Value.(type) {
		case ValueChange:
			c := b.Value.(ValueChange)
			RollbackValueChange(c)
			break
		case RelationChange:
			c := b.Value.(RelationChange)
			RollbackRelationChange(c, t.e)
			break
		}
		t.changes.Remove(b)
	}

	t.unlock()
}

func (t Transaction) Error() error {
	return t.err
}

func (t *Transaction) CreateRelation(schemaName, tableName string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, r, err := t.e.createRelation(schemaName, tableName)
	if err != nil {
		return t.abort(err)
	}

	c := RelationChange{
		schema:  s,
		current: r,
		old:     nil,
	}
	t.changes.PushBack(c)

	r.Lock()
	t.locks = append(t.locks, *r)
	return nil
}

func (t Transaction) unlock() {
	// Unlock all touched tables
	for _, r := range t.locks {
		r.Unlock()
	}
	t.locks = nil
}

func (t *Transaction) aborted() error {
	if t.err != nil {
		return fmt.Errorf("transaction aborted due to previous error: %w", t.err)
	}
	return nil
}

func (t *Transaction) abort(err error) error {
	t.Rollback()
	t.err = err
	return err
}
