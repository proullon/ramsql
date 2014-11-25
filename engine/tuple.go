package engine

// Tuple is a row in a relation
type Tuple struct {
	Values []interface{}
}

// Want to check that value are for the right Attribute and match domain
func NewTuple(values ...interface{}) *Tuple {
	t := &Tuple{}

	for _, v := range values {
		t.Values = append(t.Values, v)
	}
	return t
}

func (t *Tuple) Append(value interface{}) {
	t.Values = append(t.Values, value)
}
