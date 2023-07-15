package agnostic

import (
	"container/list"
	"sync"
)

type Relation struct {
	name   string
	schema string

	attributes []Attribute
	attrIndex  map[string]int

	// list of Tuple
	rows *list.List

	indexes []Index

	sync.RWMutex
}

func NewRelation(schema, name string) (*Relation, error) {
	r := &Relation{
		name:      name,
		schema:    schema,
		attrIndex: make(map[string]int),
		rows:      list.New(),
	}

	return r, nil
}

func (r *Relation) CreateIndex() error {
	return nil
}

func (r *Relation) Truncate() {
	r.Lock()
	defer r.Unlock()

	for _, i := range r.indexes {
		i.Truncate()
	}

	for {
		b := r.rows.Back()
		if b == nil {
			break
		}
		r.rows.Remove(b)
	}
}
