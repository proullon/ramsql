package agnostic

import (
	"fmt"
	"sync"
)

const (
	DefaultSchema = "public"
)

type Engine struct {
	schemas map[string]*Schema

	sync.Mutex
}

func NewEngine() *Engine {
	e := &Engine{}

	// create public schema
	e.schemas = make(map[string]*Schema)
	e.schemas[DefaultSchema] = NewSchema(DefaultSchema)

	return e
}

func (e *Engine) Begin() (*Transaction, error) {
	t, err := NewTransaction(e)
	return t, err
}

func (e *Engine) createRelation(schema, relation string, attributes []Attribute, pk []string) (*Schema, *Relation, error) {

	s, err := e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := NewRelation(schema, relation, attributes, pk)
	if err != nil {
		return nil, nil, err
	}

	s.Add(relation, r)

	return s, r, nil
}

func (e *Engine) dropRelation(schema, relation string) (*Schema, *Relation, error) {

	s, err := e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := s.Remove(relation)
	if err != nil {
		return nil, nil, err
	}

	return s, r, nil
}

func (e *Engine) schema(name string) (*Schema, error) {
	if name == "" {
		name = DefaultSchema
	}

	s, ok := e.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist", name)
	}

	return s, nil
}

func (e *Engine) createSchema(name string) (*Schema, error) {
	s, ok := e.schemas[name]
	if ok {
		return nil, fmt.Errorf("schema '%s' already exist", name)
	}

	s = NewSchema(name)
	e.schemas[name] = s
	return s, nil
}

func (e *Engine) dropSchema(name string) (*Schema, error) {
	s, ok := e.schemas[name]
	if !ok {
		return nil, fmt.Errorf("schema '%s' does not exist", name)
	}

	delete(e.schemas, name)
	return s, nil
}
