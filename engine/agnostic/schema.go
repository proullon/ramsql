package agnostic

import (
	"fmt"
	"sync"
)

type Schema struct {
	name      string
	relations map[string]*Relation

	sync.RWMutex
}

func NewSchema(name string) *Schema {
	s := &Schema{
		name:      name,
		relations: make(map[string]*Relation),
	}

	return s
}

func (s *Schema) Relation(name string) (*Relation, error) {
	s.RLock()
	defer s.RUnlock()

	r, ok := s.relations[name]
	if !ok {
		//	panic("lol")
		return nil, fmt.Errorf("relation '%s'.'%s' does not exist", s.name, name)
	}

	return r, nil
}

func (s *Schema) Add(name string, r *Relation) {
	s.Lock()
	defer s.Unlock()

	s.relations[name] = r
}

func (s *Schema) Remove(name string) (*Relation, error) {
	s.Lock()
	defer s.Unlock()

	r, ok := s.relations[name]
	if !ok {
		//		panic("remove")
		return nil, fmt.Errorf("relation '%s'.'%s' does not exist", s.name, name)
	}

	delete(s.relations, name)
	return r, nil
}
