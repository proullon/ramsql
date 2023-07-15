package agnostic

// Domain is the set of allowable values for an Attribute.
type Domain struct {
}

// Attribute is a named column of a relation
// AKA Field
// AKA Column
type Attribute struct {
	name          string
	typeName      string
	typeInstance  any
	defaultValue  any
	domain        Domain
	autoIncrement bool
	unique        bool
}
