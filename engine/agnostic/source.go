package agnostic

import (
	"container/list"

	"github.com/proullon/ramsql/engine/log"
)

type IndexSrc struct {
	tuple   *Tuple
	hasNext bool
}

func NewHashIndexSource(index Index, p Predicate) *IndexSrc {
	s := &IndexSrc{}

	i, ok := index.(*HashIndex)
	if !ok {
		return s
	}

	eq, ok := p.(*EqPredicate)
	if !ok {
		return s
	}

	t, err := i.Get([]any{eq.v})
	if err != nil {
		log.Debug("cannot create NewHashIndexSource(%s,%s): %s", index, p, err)
		return s
	}

	s.tuple = t
	s.hasNext = true
	return s
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

type SeqScanSrc struct {
	e *list.Element
}

func NewSeqScan(r *Relation) *SeqScanSrc {
	s := &SeqScanSrc{
		e: r.rows.Front(),
	}
	return s
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
