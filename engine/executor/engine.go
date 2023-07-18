package executor

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/proullon/ramsql/engine/agnostic"
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

func createExecutor(t *Tx, decl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {

	if len(decl.Decl) == 0 {
		return 0, 0, nil, ParsingError
	}

	if t.opsExecutors[decl.Decl[0].Token] != nil {
		return t.opsExecutors[decl.Decl[0].Token](t, decl.Decl[0])
	}

	return 0, 0, nil, NotImplemented
}

func grantExecutor(t *Tx, decl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {
	return 0, 1, nil, nil
}

func createTableExecutor(t *Tx, tableDecl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {
	var i int
	var schemaName string

	if len(tableDecl.Decl) == 0 {
		return 0, 0, nil, ParsingError
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

		_, _, _, err := t.opsExecutors[tableDecl.Decl[i].Token](t, tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, err
		}
		i++
	}

	if d, ok := tableDecl.Has(parser.SchemaToken); ok {
		schemaName = d.Lexeme
	}

	relationName := tableDecl.Decl[i].Lexeme

	// Check if 'IF NOT EXISTS' is present
	ifNotExists := hasIfNotExists(tableDecl)

	exists := t.tx.CheckRelation(schemaName, relationName)
	if exists && ifNotExists {
		return 0, 0, nil, nil
	}
	if exists {
		return 0, 0, nil, errors.New("relation already exists")
	}

	var pk []string
	var attributes []agnostic.Attribute

	// Fetch attributes
	i++
	for i < len(tableDecl.Decl) {
		attr, err := parseAttribute(tableDecl.Decl[i])
		if err != nil {
			return 0, 0, nil, err
		}
		attributes = append(attributes, attr)
		i++
	}

	err := t.tx.CreateRelation(schemaName, relationName, attributes, pk)
	if err != nil {
		return 0, 0, nil, err
	}
	return 0, 1, nil, nil
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
func insertIntoTableExecutor(t *Tx, insertDecl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {

	var lastInsertedID int64
	var schemaName string
	var returningAttrs []string
	relationName := insertDecl.Decl[0].Decl[0].Lexeme

	// Check for RETURNING clause
	if len(insertDecl.Decl) > 2 {
		for i := range insertDecl.Decl {
			if insertDecl.Decl[i].Token == parser.ReturningToken {
				returningDecl := insertDecl.Decl[i]
				returningAttrs = append(returningAttrs, returningDecl.Decl[0].Lexeme)
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
		values, err := getValues(specifiedAttrs, valueListDecl)
		if err != nil {
			return 0, 0, nil, err
		}
		tuple, err := t.tx.Insert(schemaName, relationName, values)
		if err != nil {
			return 0, 0, nil, err
		}
		tuples = append(tuples, tuple)

		// guess lastInsertedID
		if v := tuple.Values(); len(v) > 0 {
			if reflect.TypeOf(v[0]).ConvertibleTo(reflect.TypeOf(lastInsertedID)) {
				lastInsertedID = reflect.ValueOf(v[0]).Convert(reflect.TypeOf(lastInsertedID)).Int()
			}
		}
	}

	if len(returningAttrs) == 0 {
		return lastInsertedID, int64(len(tuples)), nil, nil
	}

	ch := make(chan *agnostic.Tuple, len(tuples)+2)
	collumns := &agnostic.Tuple{}
	for _, rattr := range returningAttrs {
		collumns.Append(rattr)
	}
	ch <- collumns

	for _, tuple := range tuples {
		returningTuple := &agnostic.Tuple{}
		for _, rattr := range returningAttrs {
			index, _, err := t.tx.RelationAttribute(schemaName, relationName, rattr)
			if err != nil {
				return 0, 0, nil, err
			}
			returningTuple.Append(tuple.Values()[index])
		}

		ch <- returningTuple
	}

	return lastInsertedID, int64(len(tuples)), ch, nil
}

func getValues(specifiedAttrs []string, valuesDecl *parser.Decl) (map[string]any, error) {
	var typeName string
	values := make(map[string]any)

	for i, d := range valuesDecl.Decl {
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

		v, err := agnostic.ToInstance(d.Lexeme, typeName)
		if err != nil {
			return nil, err
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
func selectExecutor(t *Tx, selectDecl *parser.Decl) (int64, int64, chan *agnostic.Tuple, error) {
	ch := make(chan *agnostic.Tuple)

	return 0, 0, ch, nil
	/*
		var attributes []Attribute
		var tables []*Table
		var predicates []PredicateLinker
		var functors []selectFunctor
		var joiners []joiner
		var schema string
		var err error

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

		err = generateVirtualRows(e, attributes, conn, schema, tables[0].name, joiners, predicates, functors)
		if err != nil {
			return err
		}

		return nil
	*/
}