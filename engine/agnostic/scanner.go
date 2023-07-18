package agnostic

import (
	"fmt"

	"github.com/proullon/ramsql/engine/log"
)

type RelationScanner struct {
	src        Source
	predicates []Predicate
}

func NewRelationScanner(src Source, predicates []Predicate) *RelationScanner {
	s := &RelationScanner{
		src:        src,
		predicates: predicates,
	}

	return s
}

func (s RelationScanner) String() string {
	return fmt.Sprintf("scan %s with %s", s.src, s.predicates)
}

func (s *RelationScanner) Append(p Predicate) {
	s.predicates = append(s.predicates, p)
}

func (s *RelationScanner) Exec() ([]string, []*Tuple, error) {
	var ok bool
	var err error
	var res []*Tuple
	var canAppend bool

	log.Debug("starting %s", s.src)

	cols := s.src.Columns()
	for s.src.HasNext() {
		t := s.src.Next()
		canAppend = true
		for _, p := range s.predicates {
			ok, err = p.Eval(cols, t)
			if !ok {
				canAppend = false
				break
			}
			if err != nil {
				return nil, nil, fmt.Errorf("RelationScanner.Exec: %s(%v) : %w", p, t, err)
			}
		}
		if canAppend {
			res = append(res, t)
		}
	}

	return cols, res, nil
}

// No idea on how to estimate cardinal of scanner given predicates
//
// min: 0
// max: len(src)
// avg: len(src)/2
func (s *RelationScanner) EstimateCardinal() int64 {
	return int64(s.src.EstimateCardinal()/2) + 1
}

func (s *RelationScanner) Children() []Node {
	return nil
}
