package agnostic

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/proullon/ramsql/engine/log"
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

var (
	NotImplemented = errors.New("not implemented")
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

// ValueFunctor is used by Predicate to compare values
//
// Possible ValueFunctor implementation:
//   - ConstValueFunctor
//   - AttributeValueFunctor
//   - NowValueFunctor
type ValueFunctor interface {
	Picker
	Value(columns []string, tuple *Tuple) any
}

// Selector defines values to be returned to user
//
// Possible Selector implementations:
//   - Attribute
//   - Star
//   - Max
//   - Min
//   - Avg
//   - ...
type Selector interface {
	Picker
	Select([]string, []*Tuple) ([]*Tuple, error)
}

// Predicate defines filter to be applied on spcified relation row
type Predicate interface {
	Picker
	Type() PredicateType
	Left() (Predicate, bool)
	Right() (Predicate, bool)
	Eval([]string, *Tuple) (bool, error)
}

type Source interface {
	HasNext() bool
	Next() *Tuple
	EstimateCardinal() int64
	Columns() []string
}

// Node is an element of a quey plan
//
// Joiner, Sorter and Scanner implement Node.
type Node interface {
	Exec() ([]string, []*Tuple, error)
	EstimateCardinal() int64
	Children() []Node
}

// Joiner joins two relation together.
//
// Should be able to estimate cardinality of join for cost optimization.
//
// Possible implementations:
//   - NaturalJoiner
//   - LeftOuterJoiner
//   - RightOuterJoiner
//   - FullOuterJoiner
type Joiner interface {
	Node
	Left() string
	SetLeft(n Node)
	Right() string
	SetRight(n Node)
}

type Joiners []Joiner

func (js Joiners) Len() int {
	return len(js)
}

func (js Joiners) Less(i, j int) bool {
	return js[i].EstimateCardinal() < js[i].EstimateCardinal()
}

func (js Joiners) Swap(i, j int) {
	js[i], js[j] = js[j], js[j]
}

// Scanner produce results by scanning the relation.
//
// The query plan initialize a Scanner for each relation with:
// * The best source possible regarding cost (Hashmap, Btree, SeqScan)
// * A (possibly) recursive predicate to filter on
type Scanner interface {
	Node
	Append(Predicate)
}

// Sorter produce a sorted result from single child node
//
// Possible implementations:
// * AscendingSort
// * DescendingSort
// * HavingSort
// * Limit ?
// * Offset ?
type Sorter interface {
	Node
}

type AttributeSelector struct {
	relation   string
	attributes []string
}

func NewAttributeSelector(rel string, attrs []string) *AttributeSelector {
	s := &AttributeSelector{
		relation:   rel,
		attributes: attrs,
	}

	return s
}

func (s AttributeSelector) String() string {
	return fmt.Sprintf("%s.%s", s.relation, s.attributes)
}

func (s *AttributeSelector) Attribute() []string {
	return s.attributes
}

func (s *AttributeSelector) Relation() string {
	return s.relation
}

func (s *AttributeSelector) Select(cols []string, in []*Tuple) (out []*Tuple, err error) {
	idx := make([]int, len(s.attributes))
	for attrIdx, attr := range s.attributes {
		idx[attrIdx] = -1
		for i, c := range cols {
			if c == s.relation+"."+attr {
				idx[attrIdx] = i
				break
			}
			if c == attr {
				idx[attrIdx] = i
				break
			}
		}
		if idx[attrIdx] == -1 {
			return nil, fmt.Errorf("AttributeSelector(%s) not found in %s", attr, cols)
		}
	}
	log.Debug("Selecting %s FROM %s (%d rows)", s.attributes, cols, len(in))

	colsLen := len(cols)
	for _, srct := range in {
		log.Debug("Select: %v", srct.values)
		if srct == nil {
			return nil, fmt.Errorf("provided tuple is nil")
		}
		if len(srct.values) != colsLen {
			return nil, fmt.Errorf("provided tuple %v does not match anounced columns %s", srct.values, cols)
		}

		t := NewTuple()
		for _, id := range idx {
			v := srct.values[id]
			t.Append(v)
		}
		out = append(out, t)
	}

	return
}

type CountSelector struct {
	relation  string
	attribute string
	cols      []string
}

func NewCountSelector(rname string, attr string) *CountSelector {
	s := &CountSelector{
		relation:  rname,
		attribute: attr,
	}
	return s
}

func (s *CountSelector) Attribute() []string {
	if s.cols != nil {
		return s.cols
	}

	if s.attribute == "*" {
		return nil
	}

	return []string{s.attribute}
}

func (s *CountSelector) Relation() string {
	return s.relation
}

func (s *CountSelector) Select(cols []string, in []*Tuple) (out []*Tuple, err error) {
	var idx int
	idx = -1
	for i, c := range cols {
		if c == s.attribute || c == s.relation+"."+s.attribute {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("%s.%s: columns not found in left node", s.relation, s.attribute)
	}

	s.cols = []string{"COUNT(" + s.attribute + ")"}
	t := NewTuple(int64(len(in)))
	out = append(out, t)
	return
}

type StarSelector struct {
	relation string
	cols     []string
}

func NewStarSelector(rname string) *StarSelector {
	s := &StarSelector{
		relation: rname,
	}
	return s
}

func (s *StarSelector) Attribute() []string {
	return s.cols
}

func (s *StarSelector) Relation() string {
	return s.relation
}

func (s *StarSelector) Select(cols []string, in []*Tuple) (out []*Tuple, err error) {
	out = in
	s.cols = cols
	return
}

type AvgSelector struct {
}

type MaxSelector struct {
}

func NewComparisonPredicate(left ValueFunctor, t PredicateType, right ValueFunctor) (Predicate, error) {

	switch t {
	case Eq:
		return NewEqPredicate(left, right), nil
	case Geq:
		return NewGeqPredicate(left, right), nil
	case Leq:
		return NewLeqPredicate(left, right), nil
	case Le:
		return NewLePredicate(left, right), nil
	case Ge:
		return NewGePredicate(left, right), nil
	case Neq:
		return NewNeqPredicate(left, right), nil
	default:
		return nil, fmt.Errorf("unknown predicate type %v", t)
	}

}

type TruePredicate struct {
}

func NewTruePredicate() *TruePredicate {
	return &TruePredicate{}
}

func (p TruePredicate) String() string {
	return "TRUE"
}

func (p *TruePredicate) Type() PredicateType {
	return True
}

func (p *TruePredicate) Eval([]string, *Tuple) (bool, error) {
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

func (p FalsePredicate) String() string {
	return "FALSE"
}

func (p *FalsePredicate) Type() PredicateType {
	return False
}

func (p *FalsePredicate) Eval([]string, *Tuple) (bool, error) {
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

func (p AndPredicate) String() string {
	return fmt.Sprintf("%s AND %s", p.left, p.right)
}

func (p *AndPredicate) Type() PredicateType {
	return And
}

func (p *AndPredicate) Eval(cols []string, t *Tuple) (bool, error) {

	l, err := p.left.Eval(cols, t)
	if err != nil {
		return false, err
	}

	r, err := p.right.Eval(cols, t)
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

type OrPredicate struct {
	left  Predicate
	right Predicate
}

func NewOrPredicate(left, right Predicate) *OrPredicate {
	p := &OrPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p OrPredicate) String() string {
	return fmt.Sprintf("%s OR %s", p.left, p.right)
}

func (p *OrPredicate) Type() PredicateType {
	return And
}

func (p *OrPredicate) Eval(cols []string, t *Tuple) (bool, error) {

	l, err := p.left.Eval(cols, t)
	if err != nil {
		return false, err
	}

	r, err := p.right.Eval(cols, t)
	if err != nil {
		return false, err
	}

	if l || r {
		return true, nil
	}

	return false, nil
}

func (p *OrPredicate) Left() (Predicate, bool) {
	return p.left, true
}

func (p *OrPredicate) Right() (Predicate, bool) {
	return p.right, true
}

func (p *OrPredicate) Relation() string {
	return ""
}

func (p *OrPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type EqPredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewEqPredicate(left, right ValueFunctor) *EqPredicate {
	p := &EqPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *EqPredicate) Type() PredicateType {
	return Eq
}

func (p EqPredicate) String() string {
	return fmt.Sprintf("%s = %s", p.left, p.right)
}

func (p *EqPredicate) Eval(cols []string, t *Tuple) (bool, error) {

	vl := p.left.Value(cols, t)
	//l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	//	r := reflect.ValueOf(vr)

	return equal(vl, vr)
}

/*
		if l.Kind() == r.Kind() {
			return l.Equal(r), nil
		}

		switch l.Kind() {
		default:
			return false, fmt.Errorf("%s not comparable", l)
		case reflect.Func, reflect.Map, reflect.Slice:
			return false, fmt.Errorf("%s not comparable", l)
		case reflect.Bool:
			if r.Kind() != reflect.Bool {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return l.Bool() == r.Bool(), nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if !r.CanInt() {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return l.Int() == r.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			if !r.CanUint() {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return l.Uint() == r.Uint(), nil
		case reflect.Float32, reflect.Float64:
			if !r.CanFloat() {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return l.Float() == r.Float(), nil
		case reflect.Complex64, reflect.Complex128:
			if !r.CanComplex() {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return l.Complex() == r.Complex(), nil
		case reflect.String:
			return l.String() == r.String(), nil
		case reflect.Chan, reflect.Pointer, reflect.UnsafePointer:
			return l.Pointer() == r.Pointer(), nil
		case reflect.Struct: // time.Time ?
			switch vl.(type) {
			case time.Time:
				ltime := vl.(time.Time)
				rtime, ok := vr.(time.Time)
				if !ok {
					return false, fmt.Errorf("%s not comparable", p)
				}
				return ltime.Unix() == rtime.Unix(), nil
			default:
				return false, fmt.Errorf("%s not comparable", p)
			}
		}
	}
*/
func (p *EqPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *EqPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *EqPredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *EqPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type SelectorNode struct {
	selectors []Selector
	child     Node
	columns   []string
}

func NewSelectorNode(selectors []Selector, n Node) *SelectorNode {
	sn := &SelectorNode{
		selectors: selectors,
		child:     n,
	}

	for _, selector := range sn.selectors {
		sn.columns = append(sn.columns, selector.Attribute()...)
	}

	return sn
}

func (sn SelectorNode) String() string {
	return fmt.Sprintf("Select %s", sn.columns)
}

func (sn *SelectorNode) Exec() ([]string, []*Tuple, error) {
	cols, srcs, err := sn.child.Exec()
	if err != nil {
		return nil, nil, err
	}

	// group by somewhere in here

	outs := make([][]*Tuple, len(sn.selectors))
	var resc []string

	var prevLen int
	for i, selector := range sn.selectors {
		out, err := selector.Select(cols, srcs)
		if err != nil {
			return nil, nil, err
		}
		outs[i] = out
		if i != 0 && len(out) != prevLen {
			return nil, nil, fmt.Errorf("selectors have different cardinals (%d and %d)", len(out), prevLen)
		}
		prevLen = len(out)
		resc = append(resc, selector.Attribute()...)
	}

	res := make([]*Tuple, prevLen)
	for i := 0; i < prevLen; i++ {
		t := NewTuple()
		for _, out := range outs {
			t.Append(out[i].values...)
		}
		res[i] = t
	}

	return resc, res, nil
}

func (sn *SelectorNode) Columns() []string {
	return sn.columns
}

func (sn *SelectorNode) EstimateCardinal() int64 {
	return sn.child.EstimateCardinal()
}

func (sn *SelectorNode) Children() []Node {
	return []Node{sn.child}
}

type NaturalJoin struct {
	leftr string
	lefta string
	left  Node

	rightr string
	righta string
	right  Node
}

func NewNaturalJoin(leftRel, leftAttr, rightRel, rightAttr string) *NaturalJoin {
	j := &NaturalJoin{
		leftr:  leftRel,
		lefta:  leftAttr,
		rightr: rightRel,
		righta: rightAttr,
	}
	return j
}

func (j NaturalJoin) String() string {
	return "JOIN " + j.leftr + "." + j.lefta + " >< " + j.rightr + "." + j.righta
}

func (j *NaturalJoin) Left() string {
	return j.leftr
}

func (j *NaturalJoin) SetLeft(n Node) {
	j.left = n
}

func (j *NaturalJoin) Right() string {
	return j.rightr
}

func (j *NaturalJoin) SetRight(n Node) {
	j.right = n
}

func (j *NaturalJoin) EstimateCardinal() int64 {
	if j.left == nil || j.right == nil {
		return 0
	}

	return int64((j.left.EstimateCardinal() * j.right.EstimateCardinal()) / 2)
}

func (j *NaturalJoin) Children() []Node {
	return []Node{j.left, j.right}
}

func (j *NaturalJoin) Exec() ([]string, []*Tuple, error) {

	log.Debug("NaturalJoin.Exec")

	lcols, lefts, err := j.left.Exec()
	if err != nil {
		return nil, nil, err
	}
	var lidx int
	lidx = -1
	for i, c := range lcols {
		if c == j.lefta || c == j.leftr+"."+j.lefta {
			lidx = i
			break
		}
	}
	if lidx == -1 {
		return nil, nil, fmt.Errorf("%s: columns not found in left node", j)
	}

	rcols, rights, err := j.right.Exec()
	if err != nil {
		return nil, nil, err
	}
	var ridx int
	ridx = -1
	for i, c := range rcols {
		if c == j.righta || c == j.rightr+"."+j.righta {
			ridx = i
			break
		}
	}
	if ridx == -1 {
		return nil, nil, fmt.Errorf("%s: columns not found in right node", j)
	}
	log.Debug("NaturalJoin.Exec: Found right (%s) %d in %v", j.righta, ridx, rcols)

	cols := make([]string, len(lcols)+len(rcols))
	var idx int
	for _, c := range lcols {
		cols[idx] = j.leftr + "." + c
		idx++
	}
	for _, c := range rcols {
		cols[idx] = j.rightr + "." + c
		idx++
	}

	log.Debug("NaturalJoin.Exec: New cols: %v", cols)

	// prepare for worst case cross join
	res := make([]*Tuple, len(lefts)*len(rights))
	idx = 0
	for _, left := range lefts {
		for _, right := range rights {
			// Need to use Eq here
			ok, err := equal(left.values[lidx], right.values[ridx])
			if err != nil {
				return nil, nil, err
			}
			if ok {
				t := NewTuple(left.values...)
				t.Append(right.values...)
				res[idx] = t
				idx++
			}
		}
	}
	res = res[:idx]

	return cols, res, nil
}

type ConstValueFunctor struct {
	v any
}

// NewConstValueFunctor creates a ValueFunctor returning v
func NewConstValueFunctor(v any) ValueFunctor {
	f := &ConstValueFunctor{
		v: v,
	}
	return f
}

func (f *ConstValueFunctor) Value([]string, *Tuple) any {
	return f.v
}

func (f *ConstValueFunctor) Relation() string {
	return ""
}

func (f *ConstValueFunctor) Attribute() []string {
	return nil
}

func (f ConstValueFunctor) String() string {
	return fmt.Sprintf("const %v (%s)", f.v, reflect.TypeOf(f.v))
}

type AttributeValueFunctor struct {
	rname string
	aname string
}

// NewAttributeValueFunctor creates a ValueFunctor returning attribute value in given tuple
func NewAttributeValueFunctor(rname, aname string) ValueFunctor {
	f := &AttributeValueFunctor{
		rname: rname,
		aname: aname,
	}

	return f
}

func (f *AttributeValueFunctor) Value(cols []string, t *Tuple) any {
	var idx = -1
	for i, c := range cols {
		if c == f.aname || c == f.rname+"."+f.aname {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}
	return t.values[idx]
}

func (f *AttributeValueFunctor) Relation() string {
	return f.rname
}

func (f *AttributeValueFunctor) Attribute() []string {
	return []string{f.aname}
}

func (f AttributeValueFunctor) String() string {
	return f.rname + "." + f.aname
}

type NowValueFunctor struct {
}

// NewNowValueFunctor creates a ValueFunctor returning time.Now()
func NewNowValueFunctor() ValueFunctor {
	f := &NowValueFunctor{}
	return f
}

func (f *NowValueFunctor) Value([]string, *Tuple) any {
	return time.Now()
}

func (f *NowValueFunctor) Relation() string {
	return ""
}

func (f *NowValueFunctor) Attribute() []string {
	return nil
}

func (f NowValueFunctor) String() string {
	return "now()"
}

type GeqPredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewGeqPredicate(left, right ValueFunctor) *GeqPredicate {
	p := &GeqPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *GeqPredicate) Type() PredicateType {
	return Geq
}

func (p GeqPredicate) String() string {
	return fmt.Sprintf("%s >= %s", p.left, p.right)
}

func (p *GeqPredicate) Eval(cols []string, t *Tuple) (bool, error) {
	vl := p.left.Value(cols, t)
	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	r := reflect.ValueOf(vr)

	switch l.Kind() {
	default:
		return false, fmt.Errorf("%s not comparable", l)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !r.CanInt() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Int() >= r.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !r.CanUint() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Uint() >= r.Uint(), nil
	case reflect.Float32, reflect.Float64:
		if !r.CanFloat() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Float() >= r.Float(), nil
	case reflect.String:
		return l.String() >= r.String(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if !ok {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return ltime.Unix() >= rtime.Unix(), nil
		default:
			return false, fmt.Errorf("%s not comparable", p)
		}
	}
}

func (p *GeqPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *GeqPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *GeqPredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *GeqPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type LeqPredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewLeqPredicate(left, right ValueFunctor) *LeqPredicate {
	p := &LeqPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *LeqPredicate) Type() PredicateType {
	return Leq
}

func (p LeqPredicate) String() string {
	return fmt.Sprintf("%s <= %s", p.left, p.right)
}

func (p *LeqPredicate) Eval(cols []string, t *Tuple) (bool, error) {
	vl := p.left.Value(cols, t)
	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	r := reflect.ValueOf(vr)

	switch l.Kind() {
	default:
		return false, fmt.Errorf("%s not comparable", l)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !r.CanInt() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Int() <= r.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !r.CanUint() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Uint() <= r.Uint(), nil
	case reflect.Float32, reflect.Float64:
		if !r.CanFloat() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Float() <= r.Float(), nil
	case reflect.String:
		return l.String() <= r.String(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if !ok {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return ltime.Unix() <= rtime.Unix(), nil
		default:
			return false, fmt.Errorf("%s not comparable", p)
		}
	}
}

func (p *LeqPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *LeqPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *LeqPredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *LeqPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type LePredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewLePredicate(left, right ValueFunctor) *LePredicate {
	p := &LePredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *LePredicate) Type() PredicateType {
	return Le
}

func (p LePredicate) String() string {
	return fmt.Sprintf("%s < %s", p.left, p.right)
}

func (p *LePredicate) Eval(cols []string, t *Tuple) (bool, error) {
	vl := p.left.Value(cols, t)
	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	r := reflect.ValueOf(vr)

	switch l.Kind() {
	default:
		return false, fmt.Errorf("%s not comparable", l)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !r.CanInt() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Int() < r.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !r.CanUint() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Uint() < r.Uint(), nil
	case reflect.Float32, reflect.Float64:
		if !r.CanFloat() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Float() < r.Float(), nil
	case reflect.String:
		return l.String() < r.String(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if !ok {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return ltime.Before(rtime), nil
		default:
			return false, fmt.Errorf("%s not comparable", p)
		}
	}
}

func (p *LePredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *LePredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *LePredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *LePredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type GePredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewGePredicate(left, right ValueFunctor) *GePredicate {
	p := &GePredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *GePredicate) Type() PredicateType {
	return Ge
}

func (p GePredicate) String() string {
	return fmt.Sprintf("%s > %s", p.left, p.right)
}

func (p *GePredicate) Eval(cols []string, t *Tuple) (bool, error) {
	vl := p.left.Value(cols, t)
	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	r := reflect.ValueOf(vr)

	switch l.Kind() {
	default:
		return false, fmt.Errorf("%s not comparable", p)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !r.CanInt() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Int() > r.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !r.CanUint() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Uint() > r.Uint(), nil
	case reflect.Float32, reflect.Float64:
		if !r.CanFloat() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Float() > r.Float(), nil
	case reflect.String:
		return l.String() > r.String(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if !ok {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return ltime.After(rtime), nil
		default:
			return false, fmt.Errorf("%s not comparable", p)
		}
	}
}

func (p *GePredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *GePredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *GePredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *GePredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

type NeqPredicate struct {
	left  ValueFunctor
	right ValueFunctor
}

func NewNeqPredicate(left, right ValueFunctor) *NeqPredicate {
	p := &NeqPredicate{
		left:  left,
		right: right,
	}

	return p
}

func (p *NeqPredicate) Type() PredicateType {
	return Neq
}

func (p NeqPredicate) String() string {
	return fmt.Sprintf("%s != %s", p.left, p.right)
}

func (p *NeqPredicate) Eval(cols []string, t *Tuple) (bool, error) {
	vl := p.left.Value(cols, t)
	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	r := reflect.ValueOf(vr)

	if l.Kind() == r.Kind() {
		return !l.Equal(r), nil
	}

	switch l.Kind() {
	default:
		return false, fmt.Errorf("%s not comparable", l)
	case reflect.Bool:
		if r.Kind() != reflect.Bool {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Bool() != r.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !r.CanInt() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Int() != r.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !r.CanUint() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Uint() != r.Uint(), nil
	case reflect.Float32, reflect.Float64:
		if !r.CanFloat() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Float() != r.Float(), nil
	case reflect.Complex64, reflect.Complex128:
		if !r.CanComplex() {
			return false, fmt.Errorf("%s not comparable", p)
		}
		return l.Complex() != r.Complex(), nil
	case reflect.String:
		return l.String() != r.String(), nil
	case reflect.Chan, reflect.Pointer, reflect.UnsafePointer:
		return l.Pointer() != r.Pointer(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if !ok {
				return false, fmt.Errorf("%s not comparable", p)
			}
			return ltime.Unix() != rtime.Unix(), nil
		default:
			return false, fmt.Errorf("%s not comparable", p)
		}
	}
}

func (p *NeqPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *NeqPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *NeqPredicate) Relation() string {
	if p.left.Relation() != "" {
		return p.left.Relation()
	}

	return p.right.Relation()
}

func (p *NeqPredicate) Attribute() []string {
	return append(p.left.Attribute(), p.right.Attribute()...)
}

func equal(vl, vr any) (bool, error) {
	l := reflect.ValueOf(vl)
	r := reflect.ValueOf(vr)

	if l.Kind() == r.Kind() {
		return l.Equal(r), nil
	}

	switch l.Kind() {
	case reflect.Bool:
		if r.Kind() == reflect.Bool {
			return l.Bool() == r.Bool(), nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if r.CanInt() {
			return l.Int() == r.Int(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if r.CanUint() {
			return l.Uint() == r.Uint(), nil
		}
	case reflect.Float32, reflect.Float64:
		if r.CanFloat() {
			return l.Float() == r.Float(), nil
		}
	case reflect.Complex64, reflect.Complex128:
		if r.CanComplex() {
			return l.Complex() == r.Complex(), nil
		}
	case reflect.String:
		return l.String() == r.String(), nil
	case reflect.Chan, reflect.Pointer, reflect.UnsafePointer:
		return l.Pointer() == r.Pointer(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if ok {
				return ltime.Unix() == rtime.Unix(), nil
			}
		}
	}

	return false, fmt.Errorf("%v (%v) and %v (%v) not comparable", vl, reflect.TypeOf(vl), vr, reflect.TypeOf(vr))
}
