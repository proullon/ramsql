package agnostic

import (
	"container/list"

	"github.com/proullon/ramsql/engine/log"
)

type IndexSrc struct {
	tuple   *Tuple
	hasNext bool
	rname   string
	cols    []string
}

func NewHashIndexSource(index Index, p Predicate) *IndexSrc {
	s := &IndexSrc{}

	i, ok := index.(*HashIndex)
	if !ok {
		return s
	}
	s.rname = i.relName
	s.cols = i.relAttrs

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
	return 1
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
