package engine

import (
// "sync"
// "database/sql"
)

// Relation is a table with column and rows
// AKA File
type Relation struct {
	// mutex sync.Mutex
	table *Table
	rows  []*Tuple
}

func NewRelation(t *Table) *Relation {
	r := &Relation{
		table: t,
	}

	return r
}

func (r *Relation) Insert(t *Tuple) error {
	// Maybe do somthing like lock read/write here
	// Maybe index
	r.rows = append(r.rows, t)
	return nil
}
