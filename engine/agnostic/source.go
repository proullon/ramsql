package agnostic

import (
	"container/list"
	"fmt"
)

type IndexSrc struct {
	tuple   *Tuple
	hasNext bool
	rname   string
	cols    []string
}

func NewHashIndexSource(index Index, p Predicate) (*IndexSrc, error) {
	s := &IndexSrc{}

	i, ok := index.(*HashIndex)
	if !ok {
		return nil, fmt.Errorf("index %s is not a HashIndex", index)
	}
	s.rname = i.relName
	s.cols = i.relAttrs

	eq, ok := p.(*EqPredicate)
	if !ok {
		return nil, fmt.Errorf("predicate %s is not a Eq predicate", p)
	}

	t, err := i.Get([]any{eq.v})
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

func (s *IndexSrc) Next() *Tuple {
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

func NewSeqScan(r *Relation) *SeqScanSrc {
	s := &SeqScanSrc{
		e:     r.rows.Front(),
		card:  int64(r.rows.Len()),
		rname: r.name,
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

func (s *SeqScanSrc) Next() *Tuple {
	if s.e == nil {
		return nil
	}
	t, _ := (s.e.Value).(*Tuple)
	s.e = s.e.Next()
	return t
}

func (s *SeqScanSrc) EstimateCardinal() int64 {
	return s.card
}

func (s *SeqScanSrc) Columns() []string {
	return s.cols
}
