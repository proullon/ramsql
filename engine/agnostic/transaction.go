package agnostic

import (
	"container/list"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/proullon/ramsql/engine/log"
)

type Transaction struct {
	e     *Engine
	locks map[string]*Relation

	// list of Change
	changes *list.List

	err error
}

func NewTransaction(e *Engine) (*Transaction, error) {
	t := Transaction{
		e:       e,
		locks:   make(map[string]*Relation),
		changes: list.New(),
	}

	return &t, nil
}

func (t *Transaction) Commit() (int, error) {
	if err := t.aborted(); err != nil {
		return 0, err
	}

	changed := t.changes.Len()

	// Remove links to be GC'd faster
	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		t.changes.Remove(b)
	}

	t.unlock()
	t.err = fmt.Errorf("transaction committed")
	return changed, nil
}

func (t *Transaction) Rollback() {
	if err := t.aborted(); err != nil {
		return
	}

	for {
		b := t.changes.Back()
		if b == nil {
			break
		}
		switch b.Value.(type) {
		case ValueChange:
			c := b.Value.(ValueChange)
			t.rollbackValueChange(c)
			break
		case RelationChange:
			c := b.Value.(RelationChange)
			t.rollbackRelationChange(c)
			break
		}
		t.changes.Remove(b)
	}

	t.unlock()
}

func (t Transaction) Error() error {
	return t.err
}

func (t *Transaction) Truncate(schema, relation string) (int64, error) {
	if err := t.aborted(); err != nil {
		return 0, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return 0, err
	}

	r, err := s.Relation(relation)
	if err != nil {
		return 0, err
	}

	c := r.Truncate()

	return c, nil
}

func (t *Transaction) RelationAttribute(schName, relName, attrName string) (int, Attribute, error) {
	if err := t.aborted(); err != nil {
		return 0, Attribute{}, err
	}

	s, err := t.e.schema(schName)
	if err != nil {
		return 0, Attribute{}, err
	}

	r, err := s.Relation(relName)
	if err != nil {
		return 0, Attribute{}, err
	}

	return r.Attribute(attrName)
}

func (t *Transaction) CheckRelation(schemaName, relName string) bool {
	if err := t.aborted(); err != nil {
		return false
	}

	s, err := t.e.schema(schemaName)
	if err != nil {
		return false
	}

	_, err = s.Relation(relName)
	if err != nil {
		return false
	}

	return true
}

func (t *Transaction) CreateRelation(schemaName, relName string, attributes []Attribute, pk []string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, r, err := t.e.createRelation(schemaName, relName, attributes, pk)
	if err != nil {
		return t.abort(err)
	}

	c := RelationChange{
		schema:  s,
		current: r,
		old:     nil,
	}
	t.changes.PushBack(c)
	log.Debug("CreateRelation(%s,%s,%s,%s)", schemaName, relName, attributes, pk)

	t.lock(r)
	return nil
}

func (t *Transaction) DropRelation(schemaName, relName string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, r, err := t.e.dropRelation(schemaName, relName)
	if err != nil {
		return t.abort(err)
	}

	c := RelationChange{
		schema:  s,
		current: nil,
		old:     r,
	}
	t.changes.PushBack(c)

	return nil
}

func (t *Transaction) CheckSchema(schemaName string) bool {
	if err := t.aborted(); err != nil {
		return false
	}

	_, err := t.e.schema(schemaName)
	if err != nil {
		return false
	}

	return true
}

func (t *Transaction) CreateSchema(schemaName string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, err := t.e.createSchema(schemaName)
	if err != nil {
		return t.abort(err)
	}

	c := SchemaChange{
		current: s,
		old:     nil,
		e:       t.e,
	}
	t.changes.PushBack(c)
	log.Debug("CreateSchema(%s)", schemaName)

	return nil
}

func (t *Transaction) DropSchema(schemaName string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, err := t.e.dropSchema(schemaName)
	if err != nil {
		return t.abort(err)
	}

	c := SchemaChange{
		current: nil,
		old:     s,
		e:       t.e,
	}
	t.changes.PushBack(c)

	return nil
}

func (t *Transaction) CreateIndex(schema, relation, index string, it IndexType, attrs []string) error {
	if err := t.aborted(); err != nil {
		return err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return err
	}

	r, err := s.Relation(relation)
	if err != nil {
		return err
	}

	t.lock(r)

	err = r.createIndex(index, it, attrs)
	if err != nil {
		return err
	}
	log.Debug("CreateIndex(%s, %s, %s, %s)", schema, relation, index, attrs)

	return nil
}

// Delete rows from relation.
//
// Delete node needs to be inserted right as child of selector node.
func (t *Transaction) Delete(schema, relation string, selectors []Selector, p Predicate) ([]string, []*Tuple, error) {
	if err := t.aborted(); err != nil {
		return nil, nil, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := s.Relation(relation)
	if err != nil {
		return nil, nil, err
	}

	n, err := t.Plan(schema, selectors, p, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	snode, ok := n.(*SelectorNode)
	if !ok {
		return nil, nil, fmt.Errorf("could not find selector node")
	}

	un := NewDeleterNode(r, t.changes)

	snode.child, un.child = un, snode.child

	log.Debug("DELETE(%s, %s, %s, %s)", schema, relation, selectors, p)
	PrintQueryPlan(n, 0, nil)

	// (4), (5), (6)
	cols, eres, err := n.Exec()
	if err != nil {
		return nil, nil, t.abort(err)
	}

	res := make([]*Tuple, len(eres))
	for i, e := range eres {
		res[i] = e.Value.(*Tuple)
	}

	return cols, res, nil
}

// Update relation with given values.
//
// Update node needs to be inserted right as child of selector node.
func (t *Transaction) Update(schema, relation string, values map[string]any, selectors []Selector, p Predicate) ([]string, []*Tuple, error) {
	if err := t.aborted(); err != nil {
		return nil, nil, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return nil, nil, err
	}

	r, err := s.Relation(relation)
	if err != nil {
		return nil, nil, err
	}

	n, err := t.Plan(schema, selectors, p, nil, nil)
	if err != nil {
		return nil, nil, err
	}

	snode, ok := n.(*SelectorNode)
	if !ok {
		return nil, nil, fmt.Errorf("could not find selector node")
	}

	un := NewUpdaterNode(r, t.changes, values)

	snode.child, un.child = un, snode.child

	log.Debug("Update(%s, %s, %s, %s, %s)", schema, relation, values, selectors, p)
	PrintQueryPlan(n, 0, nil)

	// (4), (5), (6)
	cols, eres, err := n.Exec()
	if err != nil {
		return nil, nil, t.abort(err)
	}

	res := make([]*Tuple, len(eres))
	for i, e := range eres {
		res[i] = e.Value.(*Tuple)
	}

	return cols, res, nil
}

// Build tuple for given relation
// for each column:
// - if not specified, use default value if set
// - if specified:
//   - check domain
//   - check unique
//   - check foreign key
//
// If tuple is valid, then
// - check primary key
// - insert into rows list
// - update index if any
func (t *Transaction) Insert(schema, relation string, values map[string]any) (*Tuple, error) {
	if err := t.aborted(); err != nil {
		return nil, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return nil, t.abort(err)
	}
	r, err := s.Relation(relation)
	if err != nil {
		return nil, t.abort(err)
	}

	t.lock(r)

	log.Debug("Insert into %s.%s: %v", schema, relation, values)

	tuple := &Tuple{}
	for i, attr := range r.attributes {
		val, specified := values[attr.name]
		if !specified {
			if attr.defaultValue != nil {
				tuple.Append(attr.defaultValue())
				continue
			}
			if attr.autoIncrement {
				tuple.Append(reflect.ValueOf(attr.nextValue).Convert(attr.typeInstance).Interface())
				r.attributes[i].nextValue++
				continue
			}
		}
		if specified {
			if val == nil {
				tuple.Append(val)
				delete(values, attr.name)
				continue
			}
			tof := reflect.TypeOf(val)
			if !tof.ConvertibleTo(attr.typeInstance) {
				return nil, t.abort(fmt.Errorf("cannot assign '%v' (type %s) to %s.%s (type %s)", val, tof, relation, attr.name, attr.typeInstance))
			}
			if attr.unique {
				f := NewAttributeValueFunctor(r.name, attr.name)
				p := NewEqPredicate(f, f)
				for _, index := range r.indexes {
					if ok, _ := index.CanSourceWith(p); !ok {
						continue
					}
					idx, ok := index.(*HashIndex)
					if !ok {
						return nil, t.abort(fmt.Errorf("cannot check unicity of %s", attr))
					}
					tuple, err := idx.Get([]any{val})
					if err != nil {
						return nil, t.abort(fmt.Errorf("cannot check unicity of %s", attr))
					}
					if tuple != nil {
						return nil, t.abort(fmt.Errorf("constraint violation: %s unicity", attr))
					}
				}
			}
			if attr.fk != nil {
				// TODO: predicate: equal
			}
			tuple.Append(reflect.ValueOf(val).Convert(attr.typeInstance).Interface())
			delete(values, attr.name)
			continue
		}
		return nil, t.abort(fmt.Errorf("no value for %s.%s", relation, attr.name))
	}

	// if values map is not empty, then an non existing attribute was specified
	for k, _ := range values {
		return nil, t.abort(fmt.Errorf("attribute %s does not exist in relation %s", k, relation))
	}

	// check primary key violation
	ok, err := r.CheckPrimaryKey(tuple)
	if err != nil {
		return nil, t.abort(err)
	}
	if !ok {
		return nil, t.abort(fmt.Errorf("primary key violation"))
	}

	// insert into row list
	log.Debug("Inserting %v", tuple.values)
	e := r.rows.PushBack(tuple)

	// update indexes
	for _, index := range r.indexes {
		index.Add(e)
	}

	// add change
	c := ValueChange{
		current: e,
		old:     nil,
		l:       r.rows,
	}
	t.changes.PushBack(c)

	return tuple, nil
}

// Query data from relations
//
// cf: https://en.wikipedia.org/wiki/Query_optimization
//
// cf: https://en.wikipedia.org/wiki/Relational_algebra
//
// * (1) Transaction safety : list all touched relations and lock them
// * (2) Sourcing           : evaluate which indexes query can use for each relation. HashIndex > Btree > SeqScan
// * (3) Join ordering      : estimate the cardinality (Join selection factor) of each relation after predicates filtering, then order the join by lower cardinality
// * (4) Selection          : build filtered relations on each leaf (parallelisation possible)
// * (5) Join               : join filtered relations on each node recursively
// * (6) Return result      : return result to user with selectors
//
// TODO: foreign keys should have hashmap index
func (t *Transaction) Query(schema string, selectors []Selector, p Predicate, joiners []Joiner, sorters []Sorter) ([]string, []*Tuple, error) {
	if err := t.aborted(); err != nil {
		return nil, nil, err
	}

	n, err := t.Plan(schema, selectors, p, joiners, sorters)
	if err != nil {
		return nil, nil, err
	}
	PrintQueryPlan(n, 0, nil)

	// (4), (5), (6)
	columns, eres, err := n.Exec()
	if err != nil {
		return nil, nil, t.abort(err)
	}

	res := make([]*Tuple, len(eres))
	for i, e := range eres {
		res[i] = e.Value.(*Tuple)
	}

	return columns, res, nil
}

func recAppendPredicates(rname string, sc Scanner, p Predicate) {
	if p.Relation() == rname {
		sc.Append(p)
		return
	}

	if lp, ok := p.Left(); ok {
		recAppendPredicates(rname, sc, lp)
	}
	if rp, ok := p.Right(); ok {
		recAppendPredicates(rname, sc, rp)
	}
}

func (t *Transaction) Plan(schema string, selectors []Selector, p Predicate, joiners []Joiner, sorters []Sorter) (Node, error) {
	if err := t.aborted(); err != nil {
		return nil, err
	}

	s, err := t.e.schema(schema)
	if err != nil {
		return nil, t.abort(err)
	}

	if p == nil {
		return nil, t.abort(errors.New("query requires 1 predicate"))
	}

	aliases := make(map[string]string)

	// (1)
	relations := make(map[string]*Relation)
	err = t.recLock(schema, relations, p)
	if err != nil {
		return nil, t.abort(err)
	}
	for _, sel := range selectors {
		rel := sel.Relation()
		r, err := s.Relation(rel)
		if err != nil {
			return nil, t.abort(err)
		}
		if a := sel.Alias(); a != "" {
			aliases[rel] = a
		}
		t.lock(r)
		relations[rel] = r
	}

	// (2)
	sources := make(map[string]Source)
	var sourceCost int64
	for _, r := range relations {
		for _, index := range r.indexes {
			cost, ok, p := recCanUseIndex(r.name, index, p)
			if ok && (sourceCost == 0 || cost < sourceCost) {
				log.Debug("choosing %s as source for relation %s", index, r)
				newsrc, err := NewHashIndexSource(index, getAlias(r.name, aliases), p)
				if err != nil {
					continue
				}
				sources[r.name] = newsrc
				sourceCost = cost
			}
		}
		if _, ok := sources[r.name]; !ok {
			log.Debug("could not find suitable index for relation %s, using seq scan", r)
			sources[r.name] = NewSeqScan(r, getAlias(r.name, aliases))
		}
	}

	// (3)
	// build nodes for each relations
	scanners := make(map[string]Scanner)
	for _, r := range relations {
		sc := NewRelationScanner(sources[r.name], nil)
		recAppendPredicates(r.name, sc, p)
		scanners[r.name] = sc
	}
	// assign scanner nodes to joiner nodes
	for _, j := range joiners {
		sc, ok := scanners[j.Left()]
		if !ok {
			return nil, t.abort(fmt.Errorf("cannot join %s, scanner for %s not found", j, j.Left()))
		}
		j.SetLeft(sc)
		sc, ok = scanners[j.Right()]
		if !ok {
			return nil, t.abort(fmt.Errorf("cannot join %s, scanner for %s not found", j, j.Right()))
		}
		j.SetRight(sc)
	}
	// sort joins by estimated cardinal
	sort.Sort(Joiners(joiners))
	// now we need to build tree by replacing gradually already joined relation in bigger join
	seen := make(map[string]Node)
	for _, n := range joiners {
		child, ok := seen[n.Left()]
		if !ok {
			seen[n.Left()] = n
		} else {
			n.SetLeft(child)
		}
		child, ok = seen[n.Right()]
		if !ok {
			seen[n.Right()] = n
		} else {
			n.SetRight(child)
		}
	}
	var headJoin Node
	if len(joiners) > 0 {
		headJoin = joiners[len(joiners)-1]
	} else if len(scanners) == 1 {
		// should have only on scanner then ?
		for _, v := range scanners {
			headJoin = v
		}
	} else {
		return nil, t.abort(fmt.Errorf("no join, but got %d scan", len(scanners)))
	}

	// append selectors
	n := NewSelectorNode(selectors, headJoin)

	// append sorters
	// GroupBy must contains both selector node and last join to compute arithmetic on all groups
	if sorters != nil && len(sorters) > 0 {
		sort.Sort(Sorters(sorters))
		var src Node
		for i, s := range sorters {
			if i == 0 {
				src = headJoin
			} else {
				src = sorters[i-1]
			}

			switch s.(type) {
			case *GroupBySorter:
				gb, _ := s.(*GroupBySorter)
				gb.SetNode(src)
				gb.SetSelector(n)
			default:
				s.SetNode(src)
			}
		}
		n.child = sorters[len(sorters)-1]
	}

	return n, nil
}

func (t *Transaction) recLock(schema string, relations map[string]*Relation, p Predicate) error {

	s, err := t.e.schema(schema)
	if err != nil {
		return err
	}
	if rel := p.Relation(); rel != "" {
		r, err := s.Relation(rel)
		if err != nil {
			return err
		}

		relations[p.Relation()] = r
		t.lock(r)
	}

	if lp, ok := p.Left(); ok {
		err = t.recLock(schema, relations, lp)
		if err != nil {
			return err
		}
	}
	if rp, ok := p.Right(); ok {
		err = t.recLock(schema, relations, rp)
		if err != nil {
			return err
		}
	}

	return nil
}

func recCanUseIndex(relName string, index Index, p Predicate) (int64, bool, Predicate) {
	if ok, cost := index.CanSourceWith(p); ok {
		return cost, ok, p
	}

	if lp, ok := p.Left(); ok {
		cost, ok, cp := recCanUseIndex(relName, index, lp)
		if ok {
			return cost, ok, cp
		}
	}

	if rp, ok := p.Right(); ok {
		cost, ok, cp := recCanUseIndex(relName, index, rp)
		if ok {
			return cost, ok, cp
		}
	}

	return 0, false, nil
}

// Lock relations if not already done
func (t *Transaction) lock(r *Relation) {
	_, done := t.locks[r.name]
	if done {
		return
	}

	r.Lock()
	t.locks[r.name] = r
}

// Unlock all touched relations
func (t *Transaction) unlock() {
	for _, r := range t.locks {
		r.Unlock()
	}
	t.locks = make(map[string]*Relation)
}

func (t *Transaction) aborted() error {
	if t.err != nil {
		return fmt.Errorf("transaction aborted due to previous error: %w", t.err)
	}
	return nil
}

func (t *Transaction) abort(err error) error {
	t.Rollback()
	t.err = err
	return err
}

// PrintQueryPlan
func PrintQueryPlan(n Node, depth int, printer func(fmt string, varargs ...any)) {

	if printer == nil {
		printer = log.Debug
		return
	}

	indent := ""
	for i := 0; i < depth; i++ {
		indent = fmt.Sprintf("%s    ", indent)
	}

	printer("%s|-> %s (|A| = %d)\n", indent, n, n.EstimateCardinal())
	for _, child := range n.Children() {
		PrintQueryPlan(child, depth+1, printer)
	}
}

func getAlias(name string, alias map[string]string) string {
	a, ok := alias[name]
	if ok {
		return a
	}
	return ""
}
