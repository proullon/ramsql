package agnostic

import (
	"container/list"
	"fmt"
	"strings"
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
		r.indexes = append(r.indexes, NewHashIndex("pk_"+schema+"_"+name, name, attributes, pk, r.pk))
	}

	// if unique is specified, create Hash index
	for i, a := range r.attributes {
		if a.unique {
			r.indexes = append(r.indexes, NewHashIndex("unique_"+schema+"_"+name+"_"+a.name, name, attributes, []string{a.name}, []int{i}))
		}
	}

	return r, nil
}

func (r *Relation) Attribute(name string) (int, Attribute, error) {
	name = strings.ToLower(name)
	index, ok := r.attrIndex[name]
	if !ok {
		return 0, Attribute{}, fmt.Errorf("attribute not defined: %s.%s", r.name, name)
	}
	return index, r.attributes[index], nil
}

func (r *Relation) createIndex(name string, t IndexType, attrs []string) error {

	switch t {
	case HashIndexType:
		var attrsIdx []int
		for _, a := range attrs {
			for i, rela := range r.attributes {
				if a == rela.name {
					attrsIdx = append(attrsIdx, i)
					break
				}
			}
		}
		i := NewHashIndex(name, r.name, r.attributes, attrs, attrsIdx)
		r.indexes = append(r.indexes, i)
		return nil
	case BTreeIndexType:
		return fmt.Errorf("BTree index are not implemented")
	}

	return fmt.Errorf("unknown index type: %d", t)
}

func (r *Relation) Truncate() int64 {
	r.Lock()
	defer r.Unlock()

	l := r.rows.Len()

	for _, i := range r.indexes {
		i.Truncate()
	}

	r.rows = list.New()

	return int64(l)
}

func (r Relation) String() string {
	if r.schema != "" {
		return r.schema + "." + r.name
	}
	return r.name
}
