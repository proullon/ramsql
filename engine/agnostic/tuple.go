package agnostic

// Tuple is a row in a relation
type Tuple struct {
	values []any
}

// NewTuple should check that value are for the right Attribute and match domain
func NewTuple(values ...any) *Tuple {
	t := &Tuple{}

	for _, v := range values {
		t.values = append(t.values, v)
	}
	return t
}

// Append add a value to the tuple
func (t *Tuple) Append(values ...any) {
	t.values = append(t.values, values...)
}

func (t *Tuple) Values() []any {
	return t.values
}
