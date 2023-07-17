package agnostic

import (
	"fmt"
	"reflect"
)

type PredicateType int

const (
	And PredicateType = iota
	Or
	Eq
	Geq
	Leq
	Le
	Ge
	Neq
	Like
	In
	True
	False
)

// Picker interface is used by query planner to define
// which relations and attributes are used in a query.
//
// Can be empty.
//
// Selector and Predicate implement Picker.
type Picker interface {
	Relation() string
	Attribute() []string
}

// Selector defines values to be returned to user
//
// Attribute
// Star
// Max
// Min
// Avg
// ...
type Selector interface {
	Picker
	Select([]*Tuple) ([]Tuple, error)
}

// Predicate defines filter to be applied on spcified relation row
type Predicate interface {
	Picker
	Type() PredicateType
	Left() (Predicate, bool)
	Right() (Predicate, bool)
	Eval(t *Tuple) (bool, error)
}

type Source interface {
	HasNext() bool
	Next() *Tuple
}

// Node is an element of a quey plan
//
// Joiner and Scanner implement Node.
type Node interface {
	Exec() ([]*Tuple, error)
}

// Joiner joins two relation together.
//
// Should be able to estimate cardinality of join for cost optimization.
//
// NaturalJoiner
// LeftOuterJoiner
// RightOuterJoiner
// FullOuterJoiner
type Joiner interface {
	Node
	// LeftRelation() string
	// RightRelation() string
}

// Scanner produce results by scanning the relation.
//
// The query plan initialize a Scanner for each relation with:
// * The best source possible regarding cost (Hashmap, Btree, SeqScan)
// * A (possibly) recursive predicate to filter on
type Scanner interface {
	Node
}

type AttributeSelector struct {
	Relation  string
	Attribute string
}

type CountSelector struct {
	Relation  string
	Attribute string
}

type StarSelector struct {
	relation string
}

func (s *StarSelector) Attribute() []string {
	return nil
}

func (s *StarSelector) Relation() string {
	return s.relation
}

func (s *StarSelector) Select(in []*Tuple) (out []Tuple, err error) {
	out = make([]Tuple, len(in))
	for i, t := range in {
		out[i] = *t
	}
	return
}

type AvgSelector struct {
}

type MaxSelector struct {
}

type TruePredicate struct {
}

func NewTruePredicate() *TruePredicate {
	return &TruePredicate{}
}

func (p *TruePredicate) Type() PredicateType {
	return True
}

func (p *TruePredicate) Eval(*Tuple) (bool, error) {
	return true, nil
}

func (p *TruePredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *TruePredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *TruePredicate) Relation() string {
	return ""
}

func (p *TruePredicate) Attribute() []string {
	return nil
}

type FalsePredicate struct {
}

func NewFalsePredicate() *FalsePredicate {
	return &FalsePredicate{}
}

func (p *FalsePredicate) Type() PredicateType {
	return False
}

func (p *FalsePredicate) Eval(*Tuple) (bool, error) {
	return false, nil
}

func (p *FalsePredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *FalsePredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *FalsePredicate) Relation() string {
	return ""
}

func (p *FalsePredicate) Attribute() []string {
	return nil
}

type AndPredicate struct {
	left  Predicate
	right Predicate
}

func NewAndPredicate(left, right Predicate) *AndPredicate {
	p := &AndPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *AndPredicate) Type() PredicateType {
	return And
}

func (p *AndPredicate) Eval(t *Tuple) (bool, error) {

	l, err := p.left.Eval(t)
	if err != nil {
		return false, err
	}

	r, err := p.right.Eval(t)
	if err != nil {
		return false, err
	}

	if l && r {
		return true, nil
	}

	return false, nil
}

func (p *AndPredicate) Left() (Predicate, bool) {
	return p.left, true
}

func (p *AndPredicate) Right() (Predicate, bool) {
	return p.right, true
}

func (p *AndPredicate) Relation() string {
	return ""
}

func (p *AndPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type EqPredicate struct {
	relName  string
	attrName string
	attr     int
	v        any
}

func NewEqPredicate(relName, attrName string, attr int, v any) *EqPredicate {
	p := &EqPredicate{
		relName:  relName,
		attrName: attrName,
		attr:     attr,
		v:        v,
	}

	return p
}

func (p *EqPredicate) Type() PredicateType {
	return Eq
}

func (p EqPredicate) String() string {
	return fmt.Sprintf("%s.%s=%v", p.relName, p.attrName, p.v)
}

func (p *EqPredicate) Eval(t *Tuple) (bool, error) {

	if len(t.values) <= p.attr {
		return false, fmt.Errorf("cannot eval equality for %s with tuple %v", p, t)
	}

	if reflect.DeepEqual(t.values[p.attr], p.v) {
		return true, nil
	}

	return false, nil
}

func (p *EqPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *EqPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *EqPredicate) Relation() string {
	return p.relName
}

func (p *EqPredicate) Attribute() []string {
	return []string{p.attrName}
}
