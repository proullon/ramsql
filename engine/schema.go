package engine

type Schema struct {
	name      string
	relations map[string]*Relation
}

func NewSchema(name string) *Schema {
	s := &Schema{
		name:      name,
		relations: make(map[string]*Relation),
	}

	return s
}

func (s *Schema) relation(name string) *Relation {
	r := s.relations[name]
	return r
}

func (s *Schema) add(name string, r *Relation) {
	s.relations[name] = r
}

func (s *Schema) drop(name string) {
	delete(s.relations, name)
}
