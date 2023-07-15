package engine

import (
	"fmt"
	//"github.com/proullon/ramsql/engine/parser"
	//"github.com/proullon/ramsql/engine/protocol"
)

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

func (s *Schema) relation(name string) (*Relation, error) {
	r, ok := s.relations[name]
	if !ok {
		return nil, fmt.Errorf("table '%s'.'%s' does not exist", s.name, name)
	}

	return r, nil
}

func (s *Schema) add(name string, r *Relation) {
	s.relations[name] = r
}

func (s *Schema) drop(name string) {
	delete(s.relations, name)
}

/*
func createSchemaExecutor(e *Engine, schemaDecl *parser.Decl, conn protocol.EngineConn) error {
	var name string

	if len(schemaDecl.Decl) == 0 {
		return fmt.Errorf("parsing failed, malformed query")
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(schemaDecl)

	if d, ok := schemaDecl.Has(parser.StringToken); ok {
		name = d.Lexeme
	}

	// Check if schema does not exists
	_, err := e.schema(name)
	if err == nil && !ifNotExists {
		return fmt.Errorf("schema %s already exists", name)
	}

	e.addSchema(NewSchema(name))

	conn.WriteResult(0, 1)
	return nil
}
*/
