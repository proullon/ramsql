package agnostic

import (
	"container/list"
)

type ValueChange struct {
	current *list.Element
	old     *list.Element
	l       *list.List
}

type RelationChange struct {
	schema  *Schema
	current *Relation
	old     *Relation
}

type SchemaChange struct {
	current *Schema
	old     *Schema
	e       *Engine
}

func (t *Transaction) rollbackValueChange(c ValueChange) {

	// revert insert
	if c.current != nil && c.old == nil {
		c.l.Remove(c.current)
	}

	// revert delete
	if c.current == nil && c.old != nil {
		old := c.old.Value.(*Tuple)
		c.l.InsertAfter(old, c.old.Prev())
	}

	// revert update
	if c.current != nil && c.old != nil {
		cur := c.current.Value.(*Tuple)
		old := c.old.Value.(*Tuple)
		for i, _ := range cur.values {
			cur.values[i] = old.values[i]
		}
	}
}

func (t *Transaction) rollbackRelationChange(c RelationChange) {
	// revert relation creation
	if c.current != nil && c.old == nil {
		c.schema.Remove(c.current.name)
	}

	// revert relation drop
	if c.current == nil && c.old != nil {
		c.schema.Add(c.old.name, c.old)
	}

	// revert alter
	if c.current != nil && c.old != nil {
		c.schema.Remove(c.current.name)
		c.schema.Add(c.old.name, c.old)
	}
}

func (t *Transaction) rollbackSchemaChange(c SchemaChange) {
	// revert schema creation
	if c.current != nil && c.old == nil {
		delete(c.e.schemas, c.current.name)
	}

	// revert schema drop
	if c.current == nil && c.old != nil {
		c.e.schemas[c.old.name] = c.old
	}
}
