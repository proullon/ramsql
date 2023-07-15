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
	// indexes of primary key attributes
	pk []int

	// list of Tuple
	rows *list.List

	indexes []Index

	sync.RWMutex
}

func NewRelation(schema, name string, attributes []Attribute, pk []string) (*Relation, error) {
	r := &Relation{
		name:       name,
		schema:     schema,
		attributes: attributes,
		attrIndex:  make(map[string]int),
		rows:       list.New(),
	}

	// create utils to manage attributes
	for i, a := range r.attributes {
		r.attrIndex[a.name] = i
	}
	for _, k := range pk {
		r.pk = append(r.pk, r.attrIndex[k])
	}

	// if primary key is specified, create Hash index
	if len(r.pk) != 0 {
		r.indexes = append(r.indexes, NewHashIndex(r.pk))
	}

	// if unique is specified, create Hash index
	for i, a := range r.attributes {
		if a.unique {
			r.indexes = append(r.indexes, NewHashIndex([]int{i}))
		}
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
