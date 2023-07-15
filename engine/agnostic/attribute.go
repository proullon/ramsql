package agnostic

import (
	"reflect"
	"strings"
	"time"
)

type ForeignKey struct {
	schema    string
	relation  string
	attribute string
}

// Domain is the set of allowable values for an Attribute.
type Domain struct {
}

// Attribute is a named column of a relation
// AKA Field
// AKA Column
type Attribute struct {
	name          string
	typeName      string
	typeInstance  reflect.Type
	defaultValue  any
	domain        Domain
	autoIncrement bool
	nextValue     uint64
	unique        bool
	fk            *ForeignKey
}

func NewAttribute(name, typeName string) Attribute {
	a := Attribute{
		name:         name,
		typeName:     typeName,
		typeInstance: typeInstanceFromName(typeName),
	}

	return a
}

func (a Attribute) WithAutoIncrement() Attribute {
	a.autoIncrement = true
	a.nextValue = 1
	return a
}

func (a Attribute) WithDefault(defaultValue any) Attribute {
	a.defaultValue = reflect.ValueOf(defaultValue).Convert(a.typeInstance).Interface()
	return a
}

func typeInstanceFromName(name string) reflect.Type {
	switch strings.ToLower(name) {
	case "serial", "bigserial":
		var v uint64
		return reflect.TypeOf(v)
	case "int", "bigint":
		var v int64
		return reflect.TypeOf(v)
	case "bool", "boolean":
		var v bool
		return reflect.TypeOf(v)
	case "timestamp":
		var v time.Time
		return reflect.TypeOf(v)
	default:
		var v string
		return reflect.TypeOf(v)
	}
}
