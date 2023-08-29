package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

type executorFunc func(*Tx, *parser.Decl, []NamedValue) (int64, int64, []string, []*agnostic.Tuple, error)

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
		parser.CreateToken:   createExecutor,
		parser.TableToken:    createTableExecutor,
		parser.SchemaToken:   createSchemaExecutor,
		parser.IndexToken:    createIndexExecutor,
		parser.SelectToken:   selectExecutor,
		parser.InsertToken:   insertIntoTableExecutor,
		parser.DeleteToken:   deleteExecutor,
		parser.UpdateToken:   updateExecutor,
		parser.TruncateToken: truncateExecutor,
		parser.DropToken:     dropExecutor,
		parser.GrantToken:    grantExecutor,
	}

	return t, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args []NamedValue) ([]string, []*agnostic.Tuple, error) {

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

	if t.opsExecutors[inst.Decls[0].Token] == nil {
		return nil, nil, NotImplemented
	}

	_, _, cols, res, err := t.opsExecutors[inst.Decls[0].Token](t, inst.Decls[0], args)
	if err != nil {
		return nil, nil, err
	}

	return cols, res, nil
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
	log.Info("ExecContext(%p, %s)", t.tx, query)

	instructions, err := parser.ParseInstruction(query)
	if err != nil {
		return 0, 0, err
	}

	var lastInsertedID, rowsAffected, aff int64
	for _, instruct := range instructions {
		lastInsertedID, aff, err = t.executeQuery(instruct, args)
		if err != nil {
			return 0, 0, err
		}
		rowsAffected += aff
	}

	return lastInsertedID, rowsAffected, nil
}

func (t *Tx) executeQuery(i parser.Instruction, args []NamedValue) (int64, int64, error) {

	if t.opsExecutors[i.Decls[0].Token] == nil {
		return 0, 0, NotImplemented
	}

	l, r, _, _, err := t.opsExecutors[i.Decls[0].Token](t, i.Decls[0], args)
	if err != nil {
		return 0, 0, err
	}

	return l, r, nil
}

func (t *Tx) getSelector(attr *parser.Decl, schema string, tables []string, aliases map[string]string) (agnostic.Selector, error) {
	var err error

	switch attr.Token {
	case parser.StarToken:
		return agnostic.NewStarSelector(tables[0]), nil
	case parser.CountToken:
		for _, table := range tables {
			if attr.Decl[0].Lexeme == "*" {
				return agnostic.NewCountSelector(table, "*"), nil
			}
			_, _, err = t.tx.RelationAttribute(schema, getAlias(table, aliases), attr.Decl[0].Lexeme)
			if err == nil {
				return agnostic.NewCountSelector(table, attr.Decl[0].Lexeme), nil
			}
		}
		return nil, err
	case parser.StringToken:
		attribute := attr.Lexeme
		if len(attr.Decl) > 0 {
			a := getAlias(attr.Decl[0].Lexeme, aliases)
			_, _, err = t.tx.RelationAttribute(schema, a, attribute)
			if err != nil {
				return nil, err
			}

			if attr.Decl[0].Lexeme != a {
				return agnostic.NewAttributeSelector(a, []string{attribute}, agnostic.WithAlias(attr.Decl[0].Lexeme)), nil
			}
			return agnostic.NewAttributeSelector(attr.Decl[0].Lexeme, []string{attribute}), nil
		}
		for _, table := range tables {
			_, _, err = t.tx.RelationAttribute(schema, getAlias(table, aliases), attribute)
			if err == nil {
				return agnostic.NewAttributeSelector(table, []string{attribute}), nil
			}
		}
		return nil, err
	}

	return nil, fmt.Errorf("cannot handle %s", attr.Lexeme)
}

func getSelectedTables(fromDecl *parser.Decl) (string, []string, map[string]string) {
	var tables []string
	var schema string

	aliases := make(map[string]string)

	for _, t := range fromDecl.Decl {
		schema = ""
		if d, ok := t.Has(parser.SchemaToken); ok {
			schema = d.Lexeme
		}
		if d, ok := t.Has(parser.AsToken); ok {
			aliases[d.Decl[0].Lexeme] = t.Lexeme
		}
		tables = append(tables, t.Lexeme)
	}

	return schema, tables, aliases
}

func (t *Tx) getPredicates(decl []*parser.Decl, schema, fromTableName string, args []NamedValue, aliases map[string]string) (agnostic.Predicate, error) {
	var odbcIdx int64 = 1

	for i, cond := range decl {

		if cond.Token == parser.AndToken {
			if i+1 == len(decl) {
				return nil, fmt.Errorf("query error: AND not followed by any predicate")
			}

			p, err := t.and(decl[:i], decl[i+1:], schema, fromTableName, args, aliases)
			return p, err
		}

		if cond.Token == parser.OrToken {
			if i+1 == len(decl) {
				return nil, fmt.Errorf("query error: OR not followd by any predicate")
			}
			p, err := t.or(decl[:i], decl[i+1:], schema, fromTableName, args, aliases)
			return p, err
		}
	}

	var err error
	cond := decl[0]

	// 1 PREDICATE
	if cond.Lexeme == "1" {
		log.Debug("Cond is %+v, returning TruePredicate", cond)
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

	fromTableName = getAlias(fromTableName, aliases)

	_, _, err = t.tx.RelationAttribute(schema, fromTableName, pLeftValue)
	if err != nil {
		return nil, err
	}

	// Handle IN keyword
	if cond.Decl[0].Token == parser.InToken {
		p, err := inExecutor(fromTableName, pLeftValue, cond.Decl[0])
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	// Handle NOT IN keywords
	if cond.Decl[0].Token == parser.NotToken && cond.Decl[0].Decl[0].Token == parser.InToken {
		p, err := notInExecutor(fromTableName, pLeftValue, cond.Decl[0])
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	// Handle IS NULL and IS NOT NULL
	if cond.Decl[0].Token == parser.IsToken {
		p, err := isExecutor(fromTableName, pLeftValue, cond.Decl[0])
		if err != nil {
			return nil, err
		}
		return p, nil
	}

	if len(cond.Decl) < 2 {
		return nil, fmt.Errorf("Malformed predicate \"%s\"", cond.Lexeme)
	}

	leftS := cond
	op := cond.Decl[0]
	rightS := cond.Decl[1]

	var left, right agnostic.ValueFunctor

	switch leftS.Token {
	case parser.CurrentSchemaToken:
		left = agnostic.NewConstValueFunctor(schema)
	case parser.NamedArgToken:
		for _, arg := range args {
			if leftS.Lexeme == arg.Name {
				left = agnostic.NewConstValueFunctor(arg.Value)
				break
			}
		}
		if left == nil {
			return nil, fmt.Errorf("no named argument found for '%s'", leftS.Lexeme)
		}
	case parser.ArgToken:
		var idx int64
		if rightS.Lexeme == "?" {
			idx = odbcIdx
			odbcIdx++
		} else {
			idx, err = strconv.ParseInt(rightS.Lexeme, 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if len(args) <= int(idx)-1 {
			return nil, fmt.Errorf("reference to $%s, but only %d argument provided", leftS.Lexeme, len(args))
		}
		left = agnostic.NewConstValueFunctor(args[idx-1].Value)
	default:
		left = agnostic.NewAttributeValueFunctor(fromTableName, pLeftValue)
	}

	switch rightS.Token {
	case parser.CurrentSchemaToken:
		left = agnostic.NewConstValueFunctor(schema)
	case parser.NamedArgToken:
		for _, arg := range args {
			if rightS.Lexeme == arg.Name {
				right = agnostic.NewConstValueFunctor(arg.Value)
				break
			}
		}
		if right == nil {
			return nil, fmt.Errorf("no named argument found for '%s'", rightS.Lexeme)
		}
	case parser.ArgToken:
		var idx int64
		if rightS.Lexeme == "?" {
			idx = odbcIdx
			odbcIdx++
		} else {
			idx, err = strconv.ParseInt(rightS.Lexeme, 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if len(args) <= int(idx)-1 {
			return nil, fmt.Errorf("reference to $%s, but only %d argument provided", rightS.Lexeme, len(args))
		}
		right = agnostic.NewConstValueFunctor(args[idx-1].Value)
	default:
		v, err := agnostic.ToInstance(rightS.Lexeme, parser.TypeNameFromToken(rightS.Token))
		if err != nil {
			return nil, err
		}
		right = agnostic.NewConstValueFunctor(v)
	}

	var ptype agnostic.PredicateType
	switch op.Token {
	case parser.EqualityToken:
		ptype = agnostic.Eq
	case parser.LessOrEqualToken:
		ptype = agnostic.Leq
	case parser.GreaterOrEqualToken:
		ptype = agnostic.Geq
	case parser.DistinctnessToken:
		ptype = agnostic.Neq
	case parser.LeftDipleToken:
		ptype = agnostic.Le
	case parser.RightDipleToken:
		ptype = agnostic.Ge
	default:
		return nil, fmt.Errorf("unknown comparison token %s", op.Lexeme)
	}

	return agnostic.NewComparisonPredicate(left, ptype, right)
}

func (t *Tx) and(left []*parser.Decl, right []*parser.Decl, schema, tableName string, args []NamedValue, aliases map[string]string) (agnostic.Predicate, error) {

	if len(left) == 0 {
		return nil, fmt.Errorf("no predicate before AND")
	}
	if len(right) == 0 {
		return nil, fmt.Errorf("no predicate after AND")
	}

	lp, err := t.getPredicates(left, schema, tableName, args, aliases)
	if err != nil {
		return nil, err
	}

	rp, err := t.getPredicates(right, schema, tableName, args, aliases)
	if err != nil {
		return nil, err
	}

	return agnostic.NewAndPredicate(lp, rp), nil
}

func (t *Tx) or(left []*parser.Decl, right []*parser.Decl, schema, tableName string, args []NamedValue, aliases map[string]string) (agnostic.Predicate, error) {

	if len(left) == 0 {
		return nil, fmt.Errorf("no predicate before OR")
	}
	if len(right) == 0 {
		return nil, fmt.Errorf("no predicate after OR")
	}

	lp, err := t.getPredicates(left, schema, tableName, args, aliases)
	if err != nil {
		return nil, err
	}

	rp, err := t.getPredicates(right, schema, tableName, args, aliases)
	if err != nil {
		return nil, err
	}

	return agnostic.NewOrPredicate(lp, rp), nil
}

func (t *Tx) getJoin(decl *parser.Decl, leftR string) (agnostic.Joiner, error) {
	var leftA, rightA, rightR string

	if decl.Decl[0].Token != parser.StringToken {
		return nil, fmt.Errorf("expected joined relation name, got %v", decl.Decl[0])
	}
	rightR = decl.Decl[0].Lexeme

	if decl.Decl[1].Token != parser.OnToken {
		return nil, fmt.Errorf("expected join ON information, got %v", decl.Decl[1])
	}
	on := decl.Decl[1]

	if len(on.Decl) != 3 {
		return nil, fmt.Errorf("expected JOIN ON to have pivot")
	}

	if on.Decl[0].Decl[0].Lexeme == leftR {
		leftA = on.Decl[0].Lexeme
		if len(on.Decl[0].Decl) > 0 {
			leftR = on.Decl[0].Decl[0].Lexeme
		}
		rightA = on.Decl[2].Lexeme
		if len(on.Decl[2].Decl) > 0 {
			rightR = on.Decl[2].Decl[0].Lexeme
		}
	} else {
		leftA = on.Decl[2].Lexeme
		if len(on.Decl[2].Decl) > 0 {
			leftR = on.Decl[2].Decl[0].Lexeme
		}
		rightA = on.Decl[0].Lexeme
		if len(on.Decl[0].Decl) > 0 {
			rightR = on.Decl[0].Decl[0].Lexeme
		}
	}

	return agnostic.NewNaturalJoin(leftR, leftA, rightR, rightA), nil
}

func (t *Tx) getDistinctSorter(rel string, decl *parser.Decl, nextAttr string) (agnostic.Sorter, error) {
	var dattrs []string

	// if we have ON specified
	if len(decl.Decl) > 0 {
		for _, d := range decl.Decl {
			dattrs = append(dattrs, d.Lexeme)
		}
	} else {
		// otherwise use all selected attributes
		dattrs = append(dattrs, nextAttr)
	}

	return agnostic.NewDistinctSorter(rel, dattrs), nil
}

func notInExecutor(rname string, aname string, inDecl *parser.Decl) (agnostic.Predicate, error) {
	in, err := inExecutor(rname, aname, inDecl.Decl[0])
	if err != nil {
		return nil, err
	}

	return agnostic.NewNotPredicate(in), nil
}

func inExecutor(rname string, aname string, inDecl *parser.Decl) (agnostic.Predicate, error) {

	if len(inDecl.Decl) == 0 {
		return nil, ParsingError
	}

	v := agnostic.NewAttributeValueFunctor(rname, aname)

	var n agnostic.Node
	switch inDecl.Decl[0].Token {
	case parser.SelectToken:
		return nil, fmt.Errorf("IN subquery not implemented")
	default:
		var values []any
		for _, d := range inDecl.Decl {
			values = append(values, d.Lexeme)
		}
		n = agnostic.NewListNode(values...)
	}

	p := agnostic.NewInPredicate(v, n)
	return p, nil
}

func isExecutor(rname string, aname string, isDecl *parser.Decl) (agnostic.Predicate, error) {

	if isDecl.Decl[0].Token == parser.NullToken {
		p := agnostic.NewEqPredicate(agnostic.NewAttributeValueFunctor(rname, aname), agnostic.NewConstValueFunctor(nil))
		return p, nil
	}

	if isDecl.Decl[0].Token == parser.NotToken && isDecl.Decl[1].Token == parser.NullToken {
		p := agnostic.NewEqPredicate(agnostic.NewAttributeValueFunctor(rname, aname), agnostic.NewConstValueFunctor(nil))
		return agnostic.NewNotPredicate(p), nil
	}

	return nil, ParsingError
}

func getAlias(t string, aliases map[string]string) string {
	if a, ok := aliases[t]; ok {
		return a
	}
	return t
}
