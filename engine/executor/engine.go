package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

// Engine is the root struct of RamSQL server
type Engine struct {
	memstore *agnostic.Engine
}

// New initialize a new RamSQL server
func NewEngine() (e *Engine, err error) {

	e = &Engine{
		memstore: agnostic.NewEngine(),
	}

	return
}

func (e *Engine) Begin() (*Tx, error) {
	tx, err := NewTx(context.Background(), e, sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (e *Engine) Stop() {
}

func createExecutor(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	if t.opsExecutors[decl.Decl[0].Token] != nil {
		return t.opsExecutors[decl.Decl[0].Token](t, decl.Decl[0], args)
	}

	return 0, 0, nil, nil, NotImplemented
}

func dropExecutor(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	if _, ok := decl.Has(parser.TableToken); ok {
		return dropTable(t, decl.Decl[0], args)
	}
	if _, ok := decl.Has(parser.SchemaToken); ok {
		return dropSchema(t, decl.Decl[0], args)
	}

	return 0, 0, nil, nil, NotImplemented
}

func dropTable(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	if len(decl.Decl) == 0 {
		return 0, 1, nil, nil, ParsingError
	}

	// Check if 'IF EXISTS' is present
	ifExists := hasIfExists(decl)

	rDecl := decl.Decl[0]
	if ifExists {
		rDecl = decl.Decl[1]
	}

	schema := agnostic.DefaultSchema
	if d, ok := rDecl.Has(parser.SchemaToken); ok {
		schema = d.Lexeme
	}
	relation := rDecl.Lexeme

	exists := t.tx.CheckRelation(schema, relation)
	if !exists && ifExists {
		return 0, 0, nil, nil, nil
	}
	if !exists {
		return 0, 0, nil, nil, fmt.Errorf("relation %s.%s does not exist", schema, relation)
	}

	err := t.tx.DropRelation(schema, relation)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 1, nil, nil, nil
}

func dropSchema(t *Tx, decl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	if len(decl.Decl) == 0 {
		return 0, 1, nil, nil, ParsingError
	}

	return 0, 1, nil, nil, nil
}

func grantExecutor(*Tx, *parser.Decl, []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	return 0, 1, nil, nil, nil
}

func createTableExecutor(t *Tx, tableDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {
	var i int
	var schemaName string

	if len(tableDecl.Decl) == 0 {
		return 0, 0, nil, nil, ParsingError
	}

	// Check for specific attribute
	for i < len(tableDecl.Decl) {
		if tableDecl.Decl[i].Token == parser.IfToken {
			i++
			continue
		}

		if t.opsExecutors[tableDecl.Decl[i].Token] == nil {
			break
		}

		_, _, _, _, err := t.opsExecutors[tableDecl.Decl[i].Token](t, tableDecl.Decl[i], args)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		i++
	}

	if d, ok := tableDecl.Has(parser.SchemaToken); ok {
		schemaName = d.Lexeme
	}

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	relationName := tableDecl.Decl[i].Lexeme

	exists := t.tx.CheckRelation(schemaName, relationName)
	if exists && ifNotExists {
		return 0, 0, nil, nil, nil
	}
	if exists {
		return 0, 0, nil, nil, errors.New("relation already exists")
	}

	var pk []string
	var attributes []agnostic.Attribute

	// Fetch attributes
	i++
	for i < len(tableDecl.Decl) {
		attr, isPk, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, nil, err
		}
		if isPk {
			pk = append(pk, attr.Name())
		}
		attributes = append(attributes, attr)
		i++
	}

	err := t.tx.CreateRelation(schemaName, relationName, attributes, pk)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	return 0, 1, nil, nil, nil
}

/*
|-> INSERT

	|-> INTO
	    |-> user
	        |-> last_name
	        |-> first_name
	        |-> email
	|-> VALUES
	    |-> (
	        |-> Roullon
	        |-> Pierre
	        |-> pierre.roullon@gmail.com
	|-> RETURNING
	        |-> email
*/
func insertIntoTableExecutor(t *Tx, insertDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	var lastInsertedID int64
	var schemaName string
	var returningAttrs []string
	var returningIdx []int
	relationName := insertDecl.Decl[0].Decl[0].Lexeme

	// Check for RETURNING clause
	if len(insertDecl.Decl) > 2 {
		for i := range insertDecl.Decl {
			if insertDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := insertDecl.Decl[i]
				returningAttrs = append(returningAttrs, returningDecl.Decl[0].Lexeme)
				idx, _, err := t.tx.RelationAttribute(schemaName, relationName, returningDecl.Decl[0].Lexeme)
				if err != nil {
					return 0, 0, nil, nil, fmt.Errorf("cannot return %s, doesn't exist in relation %s", returningDecl.Decl[0].Lexeme, relationName)
				}
				returningIdx = append(returningIdx, idx)
			}
		}
	}

	var specifiedAttrs []string
	for _, d := range insertDecl.Decl[0].Decl[0].Decl {
		specifiedAttrs = append(specifiedAttrs, d.Lexeme)
	}

	var tuples []*agnostic.Tuple
	valuesDecl := insertDecl.Decl[1]
	for _, valueListDecl := range valuesDecl.Decl {
		values, err := getValues(specifiedAttrs, valueListDecl, args)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		tuple, err := t.tx.Insert(schemaName, relationName, values)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		returningTuple := agnostic.NewTuple()
		for _, idx := range returningIdx {
			returningTuple.Append(tuple.Values()[idx])
		}
		tuples = append(tuples, returningTuple)

		// guess lastInsertedID
		if v := tuple.Values(); len(v) > 0 {
			if reflect.TypeOf(v[0]).ConvertibleTo(reflect.TypeOf(lastInsertedID)) {
				lastInsertedID = reflect.ValueOf(v[0]).Convert(reflect.TypeOf(lastInsertedID)).Int()
			}
		}
	}

	if len(returningAttrs) == 0 {
		return lastInsertedID, int64(len(tuples)), nil, nil, nil
	}

	return lastInsertedID, int64(len(tuples)), returningAttrs, tuples, nil
}

func getValues(specifiedAttrs []string, valuesDecl *parser.Decl, args []NamedValue) (map[string]any, error) {
	var typeName string
	var err error
	values := make(map[string]any)

	for i, d := range valuesDecl.Decl {
		if d.Lexeme == "default" || d.Lexeme == "DEFAULT" {
			continue
		}

		switch d.Token {
		case parser.IntToken, parser.NumberToken:
			typeName = "bigint"
		case parser.DateToken:
			typeName = "timestamp"
		case parser.TextToken:
			typeName = "text"
		default:
			typeName = "text"
			if _, err := agnostic.ToInstance(d.Lexeme, "timestamp"); err == nil {
				typeName = "timestamp"
			}
		}

		var v any

		switch d.Token {
		case parser.ArgToken:
			idx, err := strconv.ParseInt(d.Lexeme, 10, 64)
			if err != nil {
				return nil, err
			}
			if len(args) <= int(idx)-1 {
				return nil, fmt.Errorf("reference to $%s, but only %d argument provided", d.Lexeme, len(args))
			}
			v = args[idx-1].Value
		default:
			v, err = agnostic.ToInstance(d.Lexeme, typeName)
			if err != nil {
				return nil, err
			}
		}
		values[specifiedAttrs[i]] = v
	}

	return values, nil
}

func hasIfNotExists(tableDecl *parser.Decl) bool {
	for _, d := range tableDecl.Decl {
		if d.Token == parser.IfToken {
			if len(d.Decl) > 0 && d.Decl[0].Token == parser.NotToken {
				not := d.Decl[0]
				if len(not.Decl) > 0 && not.Decl[0].Token == parser.ExistsToken {
					return true
				}
			}
		}
	}

	return false
}

func hasIfExists(tableDecl *parser.Decl) bool {
	for _, d := range tableDecl.Decl {
		if d.Token == parser.IfToken {
			if len(d.Decl) > 0 && d.Decl[0].Token == parser.ExistsToken {
				return true
			}
		}
	}

	return false
}

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
func selectExecutor(t *Tx, selectDecl *parser.Decl, args []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error) {

	var schema string
	var selectors []agnostic.Selector
	var predicate agnostic.Predicate
	var joiners []agnostic.Joiner
	var tables []string
	var err error

	for i := range selectDecl.Decl {
		switch selectDecl.Decl[i].Token {
		case parser.FromToken:
			schema, tables = getSelectedTables(selectDecl.Decl[i])
		case parser.WhereToken:
			predicate, err = t.getPredicates(selectDecl.Decl[i].Decl, schema, tables[0], args)
			if err != nil {
				return 0, 0, nil, nil, err
			}
		case parser.JoinToken:
			j, err := t.getJoin(selectDecl.Decl[i], tables[0])
			if err != nil {
				return 0, 0, nil, nil, err
			}
			joiners = append(joiners, j)
		}
	}

	for i := range selectDecl.Decl {
		if selectDecl.Decl[i].Token != parser.StringToken &&
			selectDecl.Decl[i].Token != parser.StarToken &&
			selectDecl.Decl[i].Token != parser.CountToken {
			continue
		}
		// get attribute to select
		selector, err := t.getSelector(selectDecl.Decl[i], schema, tables)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		selectors = append(selectors, selector)
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

	log.Debug("executing '%s' with %s and %s", selectors, predicate, joiners)
	cols, res, err := t.tx.Query(schema, selectors, predicate, joiners)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	return 0, 0, cols, res, nil
}
