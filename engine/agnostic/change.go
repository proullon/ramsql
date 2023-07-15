package agnostic

type ValueChange struct {
	current *Tuple
	old     *Tuple
}

type RelationChange struct {
	schema  *Schema
	current *Relation
	old     *Relation
}

func RollbackValueChange(c ValueChange) {
	for i := range c.current.values {
		c.current.values[i] = c.old.values[i]
	}
}

func RollbackRelationChange(c RelationChange, e *Engine) {
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
