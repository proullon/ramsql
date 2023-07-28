package agnostic

import (
	"container/list"
	"fmt"
)

type IndexSrc struct {
	tuple   *list.Element
	hasNext bool
	rname   string
	cols    []string
}

func NewHashIndexSource(index Index, alias string, p Predicate) (*IndexSrc, error) {
	s := &IndexSrc{}

	i, ok := index.(*HashIndex)
	if !ok {
		return nil, fmt.Errorf("index %s is not a HashIndex", index)
	}
	s.rname = i.relName
	s.cols = i.relAttrs

	if alias != "" {
		s.rname = alias
	}

	eq, ok := p.(*EqPredicate)
	if !ok {
		return nil, fmt.Errorf("predicate %s is not a Eq predicate", p)
	}

	t, err := i.Get([]any{eq.right.Value(nil, nil)})
	if err != nil {
		return nil, fmt.Errorf("cannot create NewHashIndexSource(%s,%s): %s", index, p, err)
	}

	s.tuple = t
	if t != nil {
		s.hasNext = true
	}
	return s, nil
}

func (s IndexSrc) String() string {
	return "IndexScan on " + s.rname
}

func (s *IndexSrc) HasNext() bool {
	return s.hasNext
}

func (s *IndexSrc) Next() *list.Element {
	if !s.hasNext {
		return nil
	}
	s.hasNext = false
	return s.tuple
}

func (s *IndexSrc) Columns() []string {
	return s.cols
}

func (s *IndexSrc) EstimateCardinal() int64 {
	if s.tuple != nil {
		return 1
	}
	return 0
}

type SeqScanSrc struct {
	e     *list.Element
	card  int64
	rname string
	cols  []string
}

func NewSeqScan(r *Relation, alias string) *SeqScanSrc {
	s := &SeqScanSrc{
		e:     r.rows.Front(),
		card:  int64(r.rows.Len()),
		rname: r.name,
	}
	if alias != "" {
		s.rname = alias
	}
	for _, a := range r.attributes {
		s.cols = append(s.cols, a.name)
	}
	return s
}

func (s SeqScanSrc) String() string {
	return "SeqScan on " + s.rname
}

func (s *SeqScanSrc) HasNext() bool {
	if s.e != nil {
		return true
	}

	return false
}

func (s *SeqScanSrc) Next() *list.Element {
	if s.e == nil {
		return nil
	}
	t := s.e
	s.e = s.e.Next()
	return t
}

func (s *SeqScanSrc) EstimateCardinal() int64 {
	return s.card
}

func (s *SeqScanSrc) Columns() []string {
	return s.cols
}
