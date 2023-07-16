package agnostic

import (
	"container/list"
	"fmt"
	"reflect"
)

type Transaction struct {
	e     *Engine
	locks map[string]*Relation

	// list of Change
	changes *list.List

	err error
}

func NewTransaction(e *Engine) (*Transaction, error) {
	t := Transaction{
		e:       e,
		locks:   make(map[string]*Relation),
		changes: list.New(),
	}

	return &t, nil
}

func (t *Transaction) Commit() (int, error) {
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
	t.err = fmt.Errorf("transaction committed")
	return changed, nil
}

func (t *Transaction) Rollback() {
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

func (t *Transaction) RelationAttribute(schName, relName, attrName string) (int, Attribute, error) {
	if err := t.aborted(); err != nil {
		return 0, Attribute{}, err
	}

	s, err := t.e.schema(schName)
	if err != nil {
		return 0, Attribute{}, err
	}

	r, err := s.Relation(relName)
	if err != nil {
		return 0, Attribute{}, err
	}

	return r.Attribute(attrName)
}

func (t *Transaction) CheckRelation(schemaName, relName string) bool {
	if err := t.aborted(); err != nil {
		return false
	}

	s, err := t.e.schema(schemaName)
	if err != nil {
		return false
	}

	_, err = s.Relation(relName)
	if err != nil {
		return false
	}

	return true
}

func (t *Transaction) CreateRelation(schemaName, relName string, attributes []Attribute, pk []string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, r, err := t.e.createRelation(schemaName, relName, attributes, pk)
	if err != nil {
		return t.abort(err)
	}

	c := RelationChange{
		schema:  s,
		current: r,
		old:     nil,
	}
	t.changes.PushBack(c)

	t.lock(r)
	return nil
}

func (t *Transaction) DropRelation(schemaName, relName string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, r, err := t.e.dropRelation(schemaName, relName)
	if err != nil {
		return t.abort(err)
	}

	c := RelationChange{
		schema:  s,
		current: nil,
		old:     r,
	}
	t.changes.PushBack(c)

	return nil
}

// Build tuple for given relation
// for each column:
// - if not specified, use default value if set
// - if specified:
//   - check domain
//   - check unique
//   - check foreign key
//
// If tuple is valid, then
// - check primary key
// - insert into rows list
// - update index if any
func (t *Transaction) Insert(schema, relation string, values map[string]any) (*Tuple, error) {
	if err := t.aborted(); err != nil {
		return nil, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return nil, t.abort(err)
	}
	r, err := s.Relation(relation)
	if err != nil {
		return nil, t.abort(err)
	}

	t.lock(r)

	tuple := &Tuple{}
	for i, attr := range r.attributes {
		val, specified := values[attr.name]
		if !specified {
			if attr.defaultValue != nil {
				tuple.Append(attr.defaultValue())
				continue
			}
			if attr.autoIncrement {
				tuple.Append(reflect.ValueOf(attr.nextValue).Convert(attr.typeInstance).Interface())
				r.attributes[i].nextValue++
				continue
			}
		}
		if specified {
			tof := reflect.TypeOf(val)
			if !tof.ConvertibleTo(attr.typeInstance) {
				return nil, t.abort(fmt.Errorf("cannot assign '%v' (type %s) to %s.%s (type %s)", val, tof, relation, attr.name, attr.typeInstance))
			}
			if attr.unique {
				// TODO: predictate: equal value
				//				t.Select()
			}
			if attr.fk != nil {
				// TODO: predicate: equal
			}
			tuple.Append(reflect.ValueOf(val).Convert(attr.typeInstance).Interface())
			delete(values, attr.name)
			continue
		}
		return nil, t.abort(fmt.Errorf("no value for %s.%s", relation, attr.name))
	}

	// if values map is not empty, then an non existing attribute was specified
	for k, _ := range values {
		return nil, t.abort(fmt.Errorf("attribute %s does not exist in relation %s", k, relation))
	}

	// check primary key
	// TODO

	// insert into row list
	e := r.rows.PushBack(tuple)

	// update indexes
	for _, index := range r.indexes {
		index.Add(tuple)
	}

	// add change
	c := ValueChange{
		current: e,
		old:     nil,
		l:       r.rows,
	}
	t.changes.PushBack(c)

	return tuple, nil
}

// Lock relations if not already done
func (t *Transaction) lock(r *Relation) {
	_, done := t.locks[r.name]
	if done {
		return
	}

	r.Lock()
	t.locks[r.name] = r
}

// Unlock all touched relations
func (t *Transaction) unlock() {
	for _, r := range t.locks {
		r.Unlock()
	}
	t.locks = make(map[string]*Relation)
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
