package agnostic

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Defaulter func() any

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
	defaultValue  Defaulter
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

func (a Attribute) HasAutoIncrement() bool {
	return a.autoIncrement
}

func (a Attribute) WithDefaultConst(defaultValue any) Attribute {
	a.defaultValue = func() any {
		return reflect.ValueOf(defaultValue).Convert(a.typeInstance).Interface()
	}
	return a
}

func (a Attribute) WithDefault(defaultValue Defaulter) Attribute {
	a.defaultValue = defaultValue
	return a
}

func (a Attribute) WithUnique() Attribute {
	a.unique = true
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
	case "timestamp", "date":
		var v time.Time
		return reflect.TypeOf(v)
	default:
		var v string
		return reflect.TypeOf(v)
	}
}

func ToInstance(value, typeName string) (any, error) {
	switch strings.ToLower(typeName) {
	case "serial", "bigserial":
		var v uint64
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "int", "bigint":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "bool", "boolean":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "timestamp", "date":
		v, err := parseDate(value)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "json", "jsonb", "text", "varchar":
		v := value
		return v, nil
	default: // try everyting
		if v, err := strconv.ParseUint(value, 10, 64); err == nil {
			return v, nil
		}
		if v, err := parseDate(value); err == nil {
			return v, nil
		}
		if v, err := strconv.ParseBool(value); err == nil {
			return v, nil
		}
	}

	return nil, fmt.Errorf("cannot convert %v to instance of type %s", value, typeName)
}

func parseDate(data string) (time.Time, error) {
	DateLongFormat := "2006-01-02 15:04:05.999999999 -0700 MST"
	DateShortFormat := "2006-Jan-02"
	DateNumberFormat := "2006-01-02"

	t, err := time.Parse(DateLongFormat, data)
	if err == nil {
		return t, nil
	}

	t, err = time.Parse(time.RFC3339, data)
	if err == nil {
		return t, nil
	}

	t, err = time.Parse(DateShortFormat, data)
	if err == nil {
		return t, nil
	}

	t, err = time.Parse(DateNumberFormat, data)
	if err == nil {
		return t, nil
	}

	return time.Time{}, fmt.Errorf("cannot use '%s' as date", data)
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func NewRandString(n int) Defaulter {
	rand.Seed(time.Now().UnixNano())

	f := func() any {
		sb := strings.Builder{}
		sb.Grow(n)
		for i := 0; i < n; i++ {
			sb.WriteByte(charset[rand.Intn(len(charset))])
		}
		return sb.String()
	}

	return f
}
