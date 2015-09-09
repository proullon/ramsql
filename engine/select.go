package engine

import (
	"errors"
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

/*
|-> SELECT
	|-> *
	|-> FROM
		|-> account
	|-> WHERE
		|-> email
			|-> =
			|-> foo@bar.com
*/
func selectExecutor(e *Engine, selectDecl *parser.Decl, conn protocol.EngineConn) error {
	var attributes []Attribute
	var tables []*Table
	var predicates []Predicate
	var functors []selectFunctor
	var joiners []joiner
	var err error

	selectDecl.Stringy(0)
	for i := range selectDecl.Decl {
		switch selectDecl.Decl[i].Token {
		case parser.FromToken:
			// get selected tables
			tables = fromExecutor(selectDecl.Decl[i])
		case parser.WhereToken:
			// get WHERE declaration
			predicates, err = whereExecutor(selectDecl.Decl[i], tables[0].name)
			if err != nil {
				return err
			}
		case parser.JoinToken:
			j, err := joinExecutor(selectDecl.Decl[i])
			if err != nil {
				return err
			}
			joiners = append(joiners, j)
		case parser.OrderToken:
			// TODO: implement ORDER BY
		}
	}

	for i := range selectDecl.Decl {
		if selectDecl.Decl[i].Token != parser.StringToken &&
			selectDecl.Decl[i].Token != parser.StarToken &&
			selectDecl.Decl[i].Token != parser.CountToken {
			continue
		}

		// get attribute to selected
		attr, err := getSelectedAttribute(e, selectDecl.Decl[i], tables)
		if err != nil {
			return err
		}
		attributes = append(attributes, attr...)

	}
	// Instanciate a new select functor
	functors, err = getSelectFunctors(selectDecl)

	// and select
	// TODO: always use generateVirtualRows
	if len(joiners) != 0 {
		err = generateVirtualRows(e, attributes, conn, tables[0].name, joiners, predicates, functors)
	} else {
		err = selectRows(e, attributes, tables, conn, predicates, functors)
	}
	if err != nil {
		return err
	}

	return nil
}

type selectFunctor interface {
	Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error
	Feed(t *Tuple) error
	FeedVirtualRow(row virtualRow) error
	Done() error
}

// getSelectFunctors instanciate new functors for COUNT, MAX, MIN, AVG, ... and default select functor that return rows to client
// If a functor is specified, no attribute can be selected ?
func getSelectFunctors(attr *parser.Decl) ([]selectFunctor, error) {
	var functors []selectFunctor

	for i := range attr.Decl {

		if attr.Decl[i].Token == parser.FromToken {
			break
		}

		if attr.Decl[i].Token == parser.CountToken {
			f := &countSelectFunction{}
			functors = append(functors, f)
		}
	}

	if len(functors) == 0 {
		f := &defaultSelectFunction{}
		functors = append(functors, f)
	}

	return functors, nil

}

type defaultSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
}

func (f *defaultSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias

	return f.conn.WriteRowHeader(f.alias)
}

func (f *defaultSelectFunction) FeedVirtualRow(vrow virtualRow) error {
	var row []string

	for _, attr := range f.attributes {
		val, ok := vrow[attr]
		if !ok {
			return fmt.Errorf("could not select attribute %s", attr)
		}
		row = append(row, fmt.Sprintf("%v", val.v))
	}

	return f.conn.WriteRow(row)
}

// TODO Need disappear (see FeedVirtualRow).
func (f *defaultSelectFunction) Feed(t *Tuple) error {
	var row []string

	for _, value := range t.Values {
		row = append(row, fmt.Sprintf("%v", value))
	}
	return f.conn.WriteRow(row)
}

func (f *defaultSelectFunction) Done() error {
	return f.conn.WriteRowEnd()
}

type countSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
	Count      int64
}

func (f *countSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias
	return nil
}

func (f *countSelectFunction) Feed(t *Tuple) error {
	f.Count++
	return nil
}

func (f *countSelectFunction) FeedVirtualRow(row virtualRow) error {
	f.Count++
	return nil
}

func (f *countSelectFunction) Done() error {
	err := f.conn.WriteRowHeader(f.alias)
	if err != nil {
		return err
	}

	err = f.conn.WriteRow([]string{fmt.Sprintf("%d", f.Count)})
	if err != nil {
		return err
	}

	return f.conn.WriteRowEnd()
}

/*
   |-> WHERE
	   |-> email
		   |-> =
		   |-> foo@bar.com
*/
func whereExecutor(whereDecl *parser.Decl, fromTableName string) ([]Predicate, error) {
	var predicates []Predicate
	whereDecl.Stringy(0)

	for i := range whereDecl.Decl {
		var p Predicate
		cond := whereDecl.Decl[i]

		// 1 PREDICATE
		if whereDecl.Decl[i].Lexeme == "1" {
			predicates = append(predicates, TruePredicate)
			continue
		}

		p.LeftValue.lexeme = whereDecl.Decl[i].Lexeme
		if len(whereDecl.Decl[i].Decl) < 2 {
			return nil, fmt.Errorf("Malformed predicate \"%s\"", whereDecl.Decl[i].Lexeme)
		}

		// The first element of the list is then the relation of the attribute
		var err error
		var op *parser.Decl
		var val *parser.Decl
		var relation *parser.Decl
		if len(cond.Decl) == 3 {
			relation = cond.Decl[0]
			op = cond.Decl[1]
			val = cond.Decl[2]
		} else {
			op = cond.Decl[0]
			val = cond.Decl[1]
		}

		p.Operator, err = NewOperator(op.Token, op.Lexeme)
		if err != nil {
			return nil, err
		}
		p.RightValue.lexeme = val.Lexeme
		p.RightValue.valid = true
		if relation != nil {
			p.LeftValue.table = relation.Lexeme
		} else { // The relation is then implicitly the first table named in FROM
			p.LeftValue.table = fromTableName
		}

		predicates = append(predicates, p)
	}

	if len(predicates) == 0 {
		return nil, fmt.Errorf("No predicates provided")
	}

	return predicates, nil
}

/*
|-> FROM
	|-> account
*/
func fromExecutor(fromDecl *parser.Decl) []*Table {
	var tables []*Table
	for _, t := range fromDecl.Decl {
		tables = append(tables, NewTable(t.Lexeme))
	}

	return tables
}

func getSelectedAttribute(e *Engine, attr *parser.Decl, tables []*Table) ([]Attribute, error) {
	var attributes []Attribute

	switch attr.Token {
	case parser.StarToken:
		for _, table := range tables {
			r := e.relation(table.name)
			if r == nil {
				return nil, errors.New("Relation " + table.name + " not found")
			}
			attributes = append(attributes, r.table.attributes...)
		}
	case parser.CountToken:
		attributes = append(attributes, NewAttribute("COUNT", "int", false))
	case parser.StringToken:
		attributes = append(attributes, NewAttribute(attr.Lexeme, "text", false))
	}

	return attributes, nil
}

func selectRows(e *Engine, attr []Attribute, tables []*Table, conn protocol.EngineConn, predicates []Predicate, functors []selectFunctor) error {
	relations := make(map[string]*Relation)

	for _, t := range tables {
		r := e.relation(t.name)
		r.RLock()
		defer r.RUnlock()
		relations[t.name] = r
	}

	// Write header
	var header []string
	for _, a := range attr {
		header = append(header, a.name)
	}

	// Initialize functors here
	for i := range functors {
		if err := functors[i].Init(e, conn, header, header); err != nil {
			return err
		}
	}

	for _, relation := range relations {

		// Perform actual check of predicates on every row of tables present in FROM.
		var ok, res bool
		var err error
		for _, tuple := range relation.rows {

			ok = true
			// If the row validate all predicates, write it
			for _, predicate := range predicates {
				if res, err = predicate.Evaluate(tuple, relation.table); err != nil {
					return err
				}
				if res == false {
					ok = false
					continue
				}
			}

			if ok {
				for i := range functors {
					err := functors[i].Feed(tuple)
					if err != nil {
						return err
					}
				}
			}
		}

		for i := range functors {
			err := functors[i].Done()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
