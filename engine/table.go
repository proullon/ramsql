package engine

type Field struct {
	Name string
	Type interface{}
}

type Table struct {
	Name   string
	Fields []Field
}
