package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/parser"
)

type executorFunc func(*Tx, *parser.Decl) (int64, int64, chan *agnostic.Tuple, error)

var (
	NotImplemented = errors.New("not implemented")
	ParsingError   = errors.New("parsing error")
)

type NamedValue struct {
	Name    string
	Ordinal int
	Value   any
}

type Tx struct {
	e            *Engine
	tx           *agnostic.Transaction
	opsExecutors map[int]executorFunc
}

func NewTx(ctx context.Context, e *Engine, opts sql.TxOptions) (*Tx, error) {
	tx, err := e.memstore.Begin()
	if err != nil {
		return nil, err
	}

	t := &Tx{
		e:  e,
		tx: tx,
	}

	t.opsExecutors = map[int]executorFunc{
		parser.CreateToken: createExecutor,
		parser.TableToken:  createTableExecutor,
		//		parser.SchemaToken:   createSchemaExecutor,
		//		parser.IndexToken:    createIndexExecutor,
		parser.SelectToken: selectExecutor,
		parser.InsertToken: insertIntoTableExecutor,
		//		parser.DeleteToken:   deleteExecutor,
		//		parser.UpdateToken:   updateExecutor,
		//		parser.TruncateToken: truncateExecutor,
		//		parser.DropToken:     dropExecutor,
		parser.GrantToken: grantExecutor,
	}
	return t, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args []NamedValue) ([]string, chan *agnostic.Tuple, error) {

	instructions, err := parser.ParseInstruction(query)
	if err != nil {
		return nil, nil, err
	}
	if len(instructions) != 1 {
		return nil, nil, fmt.Errorf("expected 1 query, got %d", len(instructions))
	}

	inst := instructions[0]
	if len(inst.Decls) == 0 {
		return nil, nil, fmt.Errorf("expected 1 query")
	}
	selectDecl := inst.Decls[0]

	var schema string
	var selectors []agnostic.Selector
	var predicate agnostic.Predicate
	var joiners []agnostic.Joiner
	var tables []string

	for i := range selectDecl.Decl {
		switch selectDecl.Decl[i].Token {
		case parser.FromToken:
			schema, tables = getSelectedTables(selectDecl.Decl[i])
		case parser.WhereToken:
			predicate, err = t.getPredicates(selectDecl.Decl[i].Decl, schema, tables[0])
		}
		if err != nil {
			return nil, nil, err
		}

		for i := range selectDecl.Decl {
			if selectDecl.Decl[i].Token != parser.StringToken &&
				selectDecl.Decl[i].Token != parser.StarToken &&
				selectDecl.Decl[i].Token != parser.CountToken {
				continue
			}
			// get attribute to select
			selectors, err = t.getSelectors(selectDecl.Decl[i], schema, tables)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	/*
		for i := range selectDecl.Decl {
			switch selectDecl.Decl[i].Token {
			case parser.FromToken:
				// get selected tables
				tables = fromExecutor(selectDecl.Decl[i])
				if len(tables) > 0 {
					schema = tables[0].schema
				}
			case parser.WhereToken:
				// get WHERE declaration
				pred, err := whereExecutor2(e, selectDecl.Decl[i].Decl, schema, tables[0].name)
				if err != nil {
					return err
				}
				predicates = []PredicateLinker{pred}
			case parser.JoinToken:
				j, err := joinExecutor(selectDecl.Decl[i])
				if err != nil {
					return err
				}
				joiners = append(joiners, j)
			case parser.OrderToken:
				orderFunctor, err := orderbyExecutor(selectDecl.Decl[i], tables)
				if err != nil {
					return err
				}
				functors = append(functors, orderFunctor)
			case parser.LimitToken:
				limit, err := strconv.Atoi(selectDecl.Decl[i].Decl[0].Lexeme)
				if err != nil {
					return fmt.Errorf("wrong limit value: %s", err)
				}
				conn = limitedConn(conn, limit)
			case parser.OffsetToken:
				offset, err := strconv.Atoi(selectDecl.Decl[i].Decl[0].Lexeme)
				if err != nil {
					return fmt.Errorf("wrong offset value: %s", err)
				}
				conn = offsetedConn(conn, offset)
			case parser.DistinctToken:
				conn = distinctedConn(conn, len(selectDecl.Decl[i].Decl))
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

		if len(functors) == 0 {
			// Instantiate a new select functor
			functors, err = getSelectFunctors(selectDecl)
			if err != nil {
				return err
			}
		}
	*/

	cols, res, err := t.tx.Query(schema, selectors, predicate, joiners)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan *agnostic.Tuple, len(res))
	go func() {
		for _, r := range res {
			ch <- r
		}
		close(ch)
	}()

	return cols, ch, nil
}

// Commit the transaction on server
func (t *Tx) Commit() error {
	_, err := t.tx.Commit()
	return err
}

// Rollback all changes
func (t *Tx) Rollback() error {
	t.tx.Rollback()
	return nil
}

func (t *Tx) ExecContext(ctx context.Context, query string, args []NamedValue) (int64, int64, error) {

	instructions, err := parser.ParseInstruction(query)
	if err != nil {
		return 0, 0, err
	}

	var lastInsertedID, rowsAffected, aff int64
	for _, instruct := range instructions {
		lastInsertedID, aff, err = t.executeQuery(instruct)
		if err != nil {
			return 0, 0, err
		}
		rowsAffected += aff
	}

	return lastInsertedID, rowsAffected, nil
}

func (t *Tx) executeQuery(i parser.Instruction) (int64, int64, error) {

	/*
		i.Decls[0].Stringy(0,
		func(format string, varargs ...any) {
			fmt.Printf(format, varargs...)
		})
	*/

	if t.opsExecutors[i.Decls[0].Token] == nil {
		return 0, 0, NotImplemented
	}

	l, r, _, err := t.opsExecutors[i.Decls[0].Token](t, i.Decls[0])
	if err != nil {
		return 0, 0, err
	}

	return l, r, nil
}

func (t *Tx) getSelectors(attr *parser.Decl, schema string, tables []string) ([]agnostic.Selector, error) {
	var selectors []agnostic.Selector
	var err error
	var found bool

	switch attr.Token {
	case parser.StarToken:
		for _, table := range tables {
			selectors = append(selectors, agnostic.NewStarSelector(table))
		}
	case parser.CountToken:
		found = false
		for _, table := range tables {
			if attr.Decl[0].Lexeme == "*" {
				found = true
				break
			}
			_, _, err = t.tx.RelationAttribute(schema, table, attr.Decl[0].Lexeme)
			if err == nil {
				found = true
				selectors = append(selectors, agnostic.NewCountSelector(table, attr.Decl[0].Lexeme))
				break
			}
		}
		if !found {
			return nil, err
		}
	case parser.StringToken:
		attribute := attr.Lexeme
		if len(attr.Decl) > 0 {
			_, _, err = t.tx.RelationAttribute(schema, attr.Decl[0].Lexeme, attribute)
			if err != nil {
				return nil, err
			}
			selectors = append(selectors, agnostic.NewAttributeSelector(attr.Decl[0].Lexeme, []string{attribute}))
			break
		}
		found = false
		for _, table := range tables {
			_, _, err = t.tx.RelationAttribute(schema, table, attribute)
			if err == nil {
				found = true
				selectors = append(selectors, agnostic.NewAttributeSelector(table, []string{attribute}))
				break
			}
		}
		if !found {
			return nil, err
		}
	}

	return selectors, nil
}

func getSelectedTables(fromDecl *parser.Decl) (string, []string) {
	var tables []string
	var schema string
	for _, t := range fromDecl.Decl {
		schema = ""
		if d, ok := t.Has(parser.SchemaToken); ok {
			schema = d.Lexeme
		}
		tables = append(tables, t.Lexeme)
	}

	return schema, tables
}

func (t *Tx) getPredicates(decl []*parser.Decl, schema, fromTableName string) (agnostic.Predicate, error) {

	for i, cond := range decl {

		if cond.Token == parser.AndToken {
			if i+1 == len(decl) {
				return nil, fmt.Errorf("query error: AND not followed by any predicate")
			}

			p, err := t.and(decl[:i], decl[i+1:], schema, fromTableName)
			return p, err
		}

		if cond.Token == parser.OrToken {
			if i+1 == len(decl) {
				return nil, fmt.Errorf("query error: OR not followd by any predicate")
			}
			p, err := t.or(decl[:i], decl[i+1:], schema, fromTableName)
			return p, err
		}
	}

	var err error
	cond := decl[0]

	// 1 PREDICATE
	if cond.Lexeme == "1" {
		return agnostic.NewTruePredicate(), nil
	}

	switch cond.Decl[0].Token {
	case parser.IsToken, parser.InToken, parser.EqualityToken, parser.DistinctnessToken, parser.LeftDipleToken, parser.RightDipleToken, parser.LessOrEqualToken, parser.GreaterOrEqualToken:
		break
	default:
		fromTableName = cond.Decl[0].Lexeme
		cond.Decl = cond.Decl[1:]
		break
	}

	pLeftValue := strings.ToLower(cond.Lexeme)

	_, _, err = t.tx.RelationAttribute(schema, fromTableName, pLeftValue)
	if err != nil {
		return nil, err
	}

	// Handle IN keyword
	/*
		if cond.Decl[0].Token == parser.InToken {
			err := inExecutor(cond.Decl[0], p)
			if err != nil {
				return nil, err
			}
			p.LeftValue.table = fromTableName
			return p, nil
		}
	*/

	// Handle NOT IN keywords
	/*
		if cond.Decl[0].Token == parser.NotToken && cond.Decl[0].Decl[0].Token == parser.InToken {
			err := notInExecutor(cond.Decl[0].Decl[0], p)
			if err != nil {
				return nil, err
			}
			p.LeftValue.table = fromTableName
			return p, nil
		}
	*/

	// Handle IS NULL and IS NOT NULL
	/*
		if cond.Decl[0].Token == parser.IsToken {
			err := isExecutor(cond.Decl[0], p)
			if err != nil {
				return nil, err
			}
			p.LeftValue.table = fromTableName
			return p, nil
		}
	*/

	if len(cond.Decl) < 2 {
		return nil, fmt.Errorf("Malformed predicate \"%s\"", cond.Lexeme)
	}

	// The first element of the list is then the relation of the attribute
	op := cond.Decl[0]
	val := cond.Decl[1]

	p.Operator, err = NewOperator(op.Token, op.Lexeme)
	if err != nil {
		return nil, err
	}
	p.RightValue.lexeme = val.Lexeme
	p.RightValue.valid = true

	p.LeftValue.table = fromTableName
	return p, nil
}

func (t *Tx) and(left []*parser.Decl, right []*parser.Decl, schema, tableName string) (agnostic.Predicate, error) {

	if len(left) == 0 {
		return nil, fmt.Errorf("no predicate before AND")
	}
	if len(right) == 0 {
		return nil, fmt.Errorf("no predicate after AND")
	}

	lp, err := t.getPredicates(left, schema, tableName)
	if err != nil {
		return nil, err
	}

	rp, err := t.getPredicates(right, schema, tableName)
	if err != nil {
		return nil, err
	}

	return NewAndPredicate(left, right), nil
}

func (t *Tx) or(left []*parser.Decl, right []*parser.Decl, schema, tableName string) (agnostic.Predicate, error) {

	if len(left) == 0 {
		return nil, fmt.Errorf("no predicate before AND")
	}
	if len(right) == 0 {
		return nil, fmt.Errorf("no predicate after AND")
	}

	lp, err := t.getPredicates(left, schema, tableName)
	if err != nil {
		return nil, err
	}

	rp, err := t.getPredicates(right, schema, tableName)
	if err != nil {
		return nil, err
	}

	return NewOrPredicate(left, right), nil
}
