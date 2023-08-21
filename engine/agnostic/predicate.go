package agnostic

import (
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"sort"
	"strings"
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
	Not
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
	Alias() string
	Select([]string, []*list.Element) ([]*Tuple, error)
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
	Next() *list.Element
	EstimateCardinal() int64
	Columns() []string
}

// Node is an element of a quey plan
//
// Joiner, Sorter and Scanner implement Node.
type Node interface {
	Exec() ([]string, []*list.Element, error)
	EstimateCardinal() int64
	Children() []Node
}

type SubqueryNode struct {
	src Node
}

func NewSubqueryNode(src Node) *SubqueryNode {
	sn := &SubqueryNode{
		src: src,
	}
	return sn
}

func (sn SubqueryNode) String() string {
	buf := new(bytes.Buffer)

	PrintQueryPlan(sn.src, 1, func(format string, varargs ...any) {
		fmt.Fprintf(buf, format, varargs...)
	})

	return buf.String()
}

func (sn *SubqueryNode) Exec() ([]string, []*list.Element, error) {
	return sn.src.Exec()
}

func (sn *SubqueryNode) EstimateCardinal() int64 {
	return sn.src.EstimateCardinal()
}
func (sn *SubqueryNode) Children() []Node {
	return sn.src.Children()
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
	return js[i].EstimateCardinal() < js[j].EstimateCardinal()
}

func (js Joiners) Swap(i, j int) {
	js[i], js[j] = js[j], js[i]
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
// GroupBy (-10000) before Having (-5000) before Order (0) before Distinct (1000) before Offset (5000) before Limit (10000).
//
// # GroupBy must contains both selector node and last join to compute arithmetic on all groups
//
// Possible implementations:
//   - OrderAscSort
//   - OrderDescSort
//   - HavingSort
//   - DistinctSort
//   - Limit
//   - Offset
type Sorter interface {
	Node
	Priority() int
	SetNode(Node)
}

// Sorters sort the sorters \o/
//
// Why ? I don't want to put on package caller the responsability to order them correctly. It's up to the query planner.
type Sorters []Sorter

func (js Sorters) Len() int {
	return len(js)
}

func (js Sorters) Less(i, j int) bool {
	return js[i].Priority() < js[j].Priority()
}

func (js Sorters) Swap(i, j int) {
	js[i], js[j] = js[j], js[i]
}

type OffsetSorter struct {
	o   int
	src Node
}

func NewOffsetSorter(o int) *OffsetSorter {
	return &OffsetSorter{o: o}
}

func (s OffsetSorter) String() string {
	return fmt.Sprintf("Offset %d on %s", s.o, s.src)
}

func (s *OffsetSorter) Exec() ([]string, []*list.Element, error) {
	cols, res, err := s.src.Exec()
	if err != nil {
		return nil, nil, err
	}

	if len(res) > s.o {
		res = res[s.o:]
	}
	return cols, res, nil
}

func (s *OffsetSorter) EstimateCardinal() int64 {
	if s.src != nil {
		return int64(s.src.EstimateCardinal()/2) + 1
	}
	return 0
}

func (s *OffsetSorter) Children() []Node {
	return []Node{s.src}
}

func (s *OffsetSorter) Priority() int {
	return 5000
}

func (s *OffsetSorter) SetNode(n Node) {
	s.src = n
}

type GroupBySorter struct {
	rel      string
	attrs    []string
	src      Node
	selector Node
}

func NewGroupBySorter(rel string, attrs []string) *GroupBySorter {
	return &GroupBySorter{rel: rel, attrs: attrs}
}

func (s GroupBySorter) String() string {
	return fmt.Sprintf("GroupBy %s.%v", s.rel, s.attrs)
}

func (s *GroupBySorter) Exec() ([]string, []*list.Element, error) {
	cols, res, err := s.src.Exec()
	if err != nil {
		return nil, nil, err
	}

	var idxs []int
	for _, a := range s.attrs {
		for i, c := range cols {
			if c == a || c == s.rel+"."+a {
				idxs = append(idxs, i)
			}
		}
	}

	return cols, res, nil
}

func (s *GroupBySorter) EstimateCardinal() int64 {
	if s.src != nil {
		return int64(s.src.EstimateCardinal()/2) + 1
	}
	return 0
}

func (s *GroupBySorter) Children() []Node {
	return []Node{s.src}
}

func (s *GroupBySorter) Priority() int {
	return 0
}

func (s *GroupBySorter) SetNode(n Node) {
	s.src = n
}

func (s *GroupBySorter) SetSelector(n Node) {
	s.selector = n
}

type SortType int

const (
	ASC SortType = iota
	DESC
)

type SortExpression struct {
	attr      string
	direction SortType
}

func NewSortExpression(attr string, direction SortType) SortExpression {
	return SortExpression{attr: attr, direction: direction}
}

type OrderBySorter struct {
	rel   string
	attrs []SortExpression
	src   Node
}

func NewOrderBySorter(rel string, attrs []SortExpression) *OrderBySorter {
	s := &OrderBySorter{rel: rel, attrs: attrs}

	return s
}

func (s OrderBySorter) String() string {
	return fmt.Sprintf("OrderBy %s.%v", s.rel, s.attrs)
}

func (s *OrderBySorter) Exec() ([]string, []*list.Element, error) {
	cols, res, err := s.src.Exec()
	if err != nil {
		return nil, nil, err
	}

	var idxs []int
	for _, a := range s.attrs {
		for i, c := range cols {
			if c == a.attr || c == s.rel+"."+a.attr {
				idxs = append(idxs, i)
			}
		}
	}

	closure := func(t1idx, t2idx int) bool {
		var comp bool
		t1 := res[t1idx]
		t2 := res[t2idx]

		for i, idx := range idxs {
			v1 := t1.Value.(*Tuple).values[idx]
			v2 := t2.Value.(*Tuple).values[idx]

			eq, err := equal(v1, v2)
			if err != nil {
				log.Warn("%s: %s", s, err)
				return false
			}
			if eq {
				continue
			}

			if s.attrs[i].direction == ASC {
				comp, err = greater(v2, v1)
			} else {
				comp, err = greater(v1, v2)
			}
			if err != nil {
				log.Warn("%s: %s", s, err)
				return false
			}
			return comp
		}
		return true
	}

	sort.Slice(res, closure)
	return cols, res, nil
}

func (s *OrderBySorter) EstimateCardinal() int64 {
	if s.src != nil {
		return s.src.EstimateCardinal()
	}
	return 0
}

func (s *OrderBySorter) Children() []Node {
	return []Node{s.src}
}

func (s *OrderBySorter) Priority() int {
	return 0
}

func (s *OrderBySorter) SetNode(n Node) {
	s.src = n
}

type LimitSorter struct {
	limit int64
	src   Node
}

func NewLimitSorter(limit int64) *LimitSorter {
	return &LimitSorter{limit: limit}
}

func (s LimitSorter) String() string {
	return fmt.Sprintf("Limit %d", s.limit)
}

func (d *LimitSorter) Exec() ([]string, []*list.Element, error) {

	cols, res, err := d.src.Exec()
	if err != nil {
		return nil, nil, err
	}
	res = res[:d.limit]

	return cols, res, nil
}

func (d *LimitSorter) EstimateCardinal() int64 {
	return d.limit
}

func (d *LimitSorter) Children() []Node {
	return []Node{d.src}
}

func (d *LimitSorter) Priority() int {
	return 10000
}

func (d *LimitSorter) SetNode(n Node) {
	d.src = n
}

type DistinctSorter struct {
	rel   string
	attrs []string
	src   Node
}

func NewDistinctSorter(rel string, attrs []string) *DistinctSorter {
	return &DistinctSorter{rel: rel, attrs: attrs}
}

func (s DistinctSorter) String() string {
	return fmt.Sprintf("Distinct on %s.%v", s.rel, s.attrs)
}

func (d *DistinctSorter) Exec() ([]string, []*list.Element, error) {
	m := make(map[uint64]*list.Element)
	var h maphash.Hash
	var ok bool

	h.SetSeed(maphash.MakeSeed())

	cols, in, err := d.src.Exec()
	if err != nil {
		return nil, nil, err
	}

	var idxs []int
	for _, a := range d.attrs {
		for i, c := range cols {
			if c == a || c == d.rel+"."+a {
				idxs = append(idxs, i)
			}
		}
	}

	for _, t := range in {
		for _, idx := range idxs {
			h.Write([]byte(fmt.Sprintf("%v", t.Value.(*Tuple).values[idx])))
		}
		sum := h.Sum64()
		h.Reset()
		_, ok = m[sum]
		if !ok {
			m[sum] = t
		}
	}

	res := make([]*list.Element, len(m))
	var i int
	for _, t := range m {
		res[i] = t
		i++
	}
	return cols, res, nil
}

func (d *DistinctSorter) EstimateCardinal() int64 {
	if d.src != nil {
		return int64(d.src.EstimateCardinal()/2) + 1
	}
	return 0
}

func (d *DistinctSorter) Children() []Node {
	return []Node{d.src}
}

func (d *DistinctSorter) Priority() int {
	return 1000
}

func (d *DistinctSorter) SetNode(n Node) {
	d.src = n
}

type AttributeSelector struct {
	relation   string
	attributes []string
	alias      string
}

func NewAttributeSelector(rel string, attrs []string, functors ...func(*AttributeSelector)) *AttributeSelector {
	s := &AttributeSelector{
		relation:   rel,
		attributes: attrs,
	}

	for _, f := range functors {
		f(s)
	}

	return s
}

func WithAlias(alias string) func(*AttributeSelector) {
	return func(s *AttributeSelector) {
		s.alias = alias
		attr := s.attributes
		s.attributes = nil
		for _, a := range attr {
			s.attributes = append(s.attributes, s.alias+"."+a)
		}
	}
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

func (s *AttributeSelector) Alias() string {
	return s.alias
}

func (s *AttributeSelector) Select(cols []string, in []*list.Element) (out []*Tuple, err error) {
	idx := make([]int, len(s.attributes))
	for attrIdx, attr := range s.attributes {
		idx[attrIdx] = -1
		lattr := strings.ToLower(attr)
		for i, c := range cols {
			lc := strings.ToLower(c)
			if lc == lattr {
				idx[attrIdx] = i
				break
			}
			if lc == s.relation+"."+lattr {
				idx[attrIdx] = i
				break
			}
			if s.alias+"."+lc == lattr {
				idx[attrIdx] = i
				break
			}
		}
		if idx[attrIdx] == -1 {
			return nil, fmt.Errorf("AttributeSelector(%s) not found in %s", attr, cols)
		}
	}

	colsLen := len(cols)
	for _, e := range in {
		if e == nil {
			return nil, fmt.Errorf("provided tuple is nil")
		}
		srct := e.Value.(*Tuple)
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
	alias     string
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

func (s *CountSelector) Alias() string {
	return s.alias
}

func (s *CountSelector) Select(cols []string, in []*list.Element) (out []*Tuple, err error) {
	var idx int
	idx = -1
	for i, c := range cols {
		if s.attribute == "*" || c == s.attribute || c == s.relation+"."+s.attribute {
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
	alias    string
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

func (s *StarSelector) Alias() string {
	return s.alias
}

func (s *StarSelector) Select(cols []string, in []*list.Element) (out []*Tuple, err error) {
	var colIdx []int

	// if only 1 relation, can return directly
	for i, c := range cols {
		if strings.Contains(c, ".") == false {
			out = make([]*Tuple, len(in))
			for i, e := range in {
				t, ok := e.Value.(*Tuple)
				if !ok {
					return nil, fmt.Errorf("provided element list does not contain Tuple")
				}
				out[i] = t
			}
			s.cols = cols
			return
		}
		if strings.HasPrefix(c, s.relation) {
			s.cols = append(s.cols, strings.Split(c, ".")[1])
			colIdx = append(colIdx, i)
		}
	}
	// need to re-select table
	for _, e := range in {
		intup := e.Value.(*Tuple)
		outtup := &Tuple{values: make([]any, len(colIdx))}
		for i, idx := range colIdx {
			outtup.values[i] = intup.values[idx]
		}
		out = append(out, outtup)
	}
	return
}

func (s StarSelector) String() string {
	return s.relation + ".*"
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

type NotPredicate struct {
	src Predicate
}

func NewNotPredicate(src Predicate) *NotPredicate {
	return &NotPredicate{src: src}
}

func (p NotPredicate) String() string {
	return fmt.Sprintf("NOT %s", p.src)
}

func (p *NotPredicate) Type() PredicateType {
	return Not
}

func (p *NotPredicate) Eval(cols []string, t *Tuple) (bool, error) {

	e, err := p.src.Eval(cols, t)
	if err != nil {
		return false, err
	}

	return !e, nil
}

func (p *NotPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *NotPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *NotPredicate) Relation() string {
	return p.src.Relation()
}

func (p *NotPredicate) Attribute() []string {
	return p.src.Attribute()
}

type InPredicate struct {
	v    ValueFunctor
	src  Node
	cols []string
	res  []*Tuple
}

func NewInPredicate(v ValueFunctor, src Node) *InPredicate {
	p := &InPredicate{v: v, src: src}
	return p
}

func (p InPredicate) String() string {
	return fmt.Sprintf("%s IN %s", p.v, p.src)
}

func (p *InPredicate) Type() PredicateType {
	return In
}

func (p *InPredicate) Eval(inCols []string, in *Tuple) (bool, error) {

	if p.res == nil {
		cols, res, err := p.src.Exec()
		if err != nil {
			return false, err
		}
		p.cols = cols
		for _, e := range res {
			p.res = append(p.res, e.Value.(*Tuple))
		}
	}

	lv := p.v.Value(inCols, in)

	for _, t := range p.res {
		rv := t.values[0]
		eq, err := equal(lv, rv)
		if eq {
			return true, nil
		}
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

func (p *InPredicate) Left() (Predicate, bool) {
	return nil, false
}

func (p *InPredicate) Right() (Predicate, bool) {
	return nil, false
}

func (p *InPredicate) Relation() string {
	return p.v.Relation()
}

func (p *InPredicate) Attribute() []string {
	return p.v.Attribute()
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
	if p.left != nil && p.right != nil && p.left.Relation() == p.right.Relation() {
		return p.left.Relation()
	}
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
	if p.left != nil && p.right != nil && p.left.Relation() == p.right.Relation() {
		return p.left.Relation()
	}
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
	vr := p.right.Value(cols, t)

	return equal(vl, vr)
}

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

type ListNode struct {
	res []*list.Element
}

func NewListNode(values ...any) *ListNode {
	n := &ListNode{}

	list := list.New()

	for _, v := range values {
		e := list.PushBack(NewTuple(v))
		n.res = append(n.res, e)
	}

	return n
}

func (ln ListNode) String() string {
	return fmt.Sprintf("CONST_LIST (%d)", len(ln.res))
}

func (ln *ListNode) Exec() ([]string, []*list.Element, error) {
	return []string{""}, ln.res, nil
}

func (ln *ListNode) EstimateCardinal() int64 {
	return int64(len(ln.res))
}

func (ln *ListNode) Children() []Node {
	return nil
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

func (sn *SelectorNode) Exec() ([]string, []*list.Element, error) {
	cols, srcs, err := sn.child.Exec()
	if err != nil {
		return nil, nil, err
	}
	if len(sn.selectors) == 0 {
		return cols, srcs, nil
	}

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

	// We have prevLen rows with l columns to return
	res := make([]*list.Element, prevLen)
	l := len(resc)
	rl := list.New()
	// for each new tuple, concatenate values returned by all selectors
	for i := 0; i < prevLen; i++ {
		t := &Tuple{values: make([]any, l)}
		var tidx int
		// for each selector returned Tuple
		for x, _ := range outs {
			// concatenate values to unified tuple
			for y, _ := range outs[x][i].values {
				t.values[tidx] = outs[x][i].values[y]
				tidx++
			}
		}

		e := rl.PushBack(t)
		res[i] = e
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

func (j *NaturalJoin) Exec() ([]string, []*list.Element, error) {

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
	log.Debug("NaturalJoin.Exec: Found left (%s) %d in %v", j.lefta, lidx, lcols)

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
		if strings.Contains(c, ".") {
			cols[idx] = c
		} else {
			cols[idx] = j.leftr + "." + c
		}
		idx++
	}
	for _, c := range rcols {
		if strings.Contains(c, ".") {
			cols[idx] = c
		} else {
			cols[idx] = j.rightr + "." + c
		}
		idx++
	}

	log.Debug("NaturalJoin.Exec: New cols: %v", cols)

	// prepare for worst case cross join
	l := list.New()
	for _, left := range lefts {
		for _, right := range rights {
			ok, err := equal(left.Value.(*Tuple).values[lidx], right.Value.(*Tuple).values[ridx])
			if err != nil {
				return nil, nil, err
			}
			if ok {
				t := NewTuple(left.Value.(*Tuple).values...)
				t.Append(right.Value.(*Tuple).values...)
				l.PushBack(t)
			}
		}
	}
	idx = 0
	res := make([]*list.Element, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		res[idx] = e
		idx++
	}

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

	if vl == nil && vr == nil {
		return true, nil
	}
	if vl == nil || vr == nil {
		return false, nil
	}

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

	if vl == nil && vr == nil {
		return true, nil
	}
	if vl == nil || vr == nil {
		return false, nil
	}

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

	if vl == nil && vr == nil {
		return false, nil
	}
	if vl == nil || vr == nil {
		return false, nil
	}

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
	//	l := reflect.ValueOf(vl)
	vr := p.right.Value(cols, t)
	//	r := reflect.ValueOf(vr)

	return greater(vl, vr)
}

/*
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
*/

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

	if vl == nil && vr == nil {
		return false, nil
	}
	if vl == nil || vr == nil {
		return true, nil
	}

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

	if vl == nil && vr == nil {
		return true, nil
	}
	if vl == nil || vr == nil {
		return false, nil
	}

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

func greater(vl, vr any) (bool, error) {
	l := reflect.ValueOf(vl)
	r := reflect.ValueOf(vr)

	if vl == nil && vr == nil {
		return false, nil
	}
	if vl == nil {
		return false, nil
	}
	if vr == nil {
		return true, nil
	}

	switch l.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if r.CanInt() {
			return l.Int() > r.Int(), nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if r.CanUint() {
			return l.Uint() > r.Uint(), nil
		}
	case reflect.Float32, reflect.Float64:
		if r.CanFloat() {
			return l.Float() > r.Float(), nil
		}
	case reflect.String:
		return l.String() > r.String(), nil
	case reflect.Struct: // time.Time ?
		switch vl.(type) {
		case time.Time:
			ltime := vl.(time.Time)
			rtime, ok := vr.(time.Time)
			if ok {
				return ltime.After(rtime), nil
			}
		}
	}

	return false, fmt.Errorf("%v (%v) and %v (%v) not comparable", vl, reflect.TypeOf(vl), vr, reflect.TypeOf(vr))
}

type Updater struct {
	rel        string
	rows       *list.List
	changes    *list.List
	values     map[string]any
	attrs      []string
	child      Node
	attributes []Attribute
	indexes    []Index
}

func NewUpdaterNode(relation *Relation, changes *list.List, values map[string]any) *Updater {
	u := &Updater{
		rel:        relation.name,
		rows:       relation.rows,
		changes:    changes,
		values:     values,
		attributes: relation.attributes,
		indexes:    relation.indexes,
	}

	for k, _ := range values {
		u.attrs = append(u.attrs, strings.ToLower(k))
	}
	return u
}

func (u Updater) String() string {
	return fmt.Sprintf("UPDATE on %s values %s with %s", u.rel, u.values, u.child)
}

func (u *Updater) Children() []Node {
	return []Node{u.child}
}

func (u *Updater) EstimateCardinal() int64 {
	return u.child.EstimateCardinal()
}

func (u *Updater) Exec() (cols []string, out []*list.Element, err error) {
	var in []*list.Element

	cols, in, err = u.child.Exec()
	if err != nil {
		return nil, nil, err
	}

	for _, e := range in {
		t := e.Value.(*Tuple)

		newt := &Tuple{
			values: make([]any, len(t.values)),
		}

		for i, v := range t.values {
			nv := v
			attr := u.attributes[i]
			if val, ok := u.values[cols[i]]; ok {
				if val == nil {
					newt.values[i] = nil
					delete(u.values, cols[i])
					continue
				}
				tof := reflect.TypeOf(val)
				if !tof.ConvertibleTo(attr.typeInstance) {
					return nil, nil, fmt.Errorf("cannot assign '%v' (type %s) to %s.%s (type %s)", val, tof, u.rel, attr.name, attr.typeInstance)
				}
				nv = reflect.ValueOf(val).Convert(attr.typeInstance).Interface()
				log.Debug("Updating %s to %v", attr.name, nv)
			}

			newt.values[i] = nv
			delete(u.values, cols[i])
		}

		newe := u.rows.InsertAfter(newt, e)
		if newe == nil {
			return nil, nil, fmt.Errorf("cannot update rows %v with %v, element not in rows", e, newe)
		}
		u.rows.Remove(e)
		for _, i := range u.indexes {
			i.Remove(e)
		}
		for _, i := range u.indexes {
			i.Add(newe)
		}
		out = append(out, newe)

		c := &ValueChange{
			current: newe,
			old:     e,
			l:       u.rows,
		}
		u.changes.PushBack(c)
	}

	if len(u.values) > 0 {
		return nil, nil, fmt.Errorf("attribute %s not existing in relation %s, %s", u.values, u.rel, u.attributes)
	}
	return cols, out, nil
}

func (u *Updater) Relation() string {
	return u.rel
}

func (u *Updater) Attribute() []string {
	return u.attrs
}

type Deleter struct {
	rel        string
	rows       *list.List
	changes    *list.List
	child      Node
	attributes []Attribute
	indexes    []Index
}

func NewDeleterNode(relation *Relation, changes *list.List) *Deleter {
	u := &Deleter{
		rel:        relation.name,
		rows:       relation.rows,
		changes:    changes,
		attributes: relation.attributes,
		indexes:    relation.indexes,
	}

	return u
}

func (u Deleter) String() string {
	return fmt.Sprintf("DELETE on %s with %s", u.rel, u.child)
}

func (u *Deleter) Children() []Node {
	return []Node{u.child}
}

func (u *Deleter) EstimateCardinal() int64 {
	return u.child.EstimateCardinal()
}

func (u *Deleter) Exec() (cols []string, out []*list.Element, err error) {
	var in []*list.Element

	cols, in, err = u.child.Exec()
	if err != nil {
		return nil, nil, err
	}

	for _, t := range in {

		u.rows.Remove(t)
		for _, i := range u.indexes {
			i.Remove(t)
		}

		out = append(out, t)

		c := &ValueChange{
			current: nil,
			old:     t,
			l:       u.rows,
		}
		u.changes.PushBack(c)
	}

	return cols, out, nil
}

func (u *Deleter) Relation() string {
	return u.rel
}

func (u *Deleter) Attribute() []string {
	// return u.child.Attribute()
	return []string{}
}
