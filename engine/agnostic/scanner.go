package agnostic

import (
	"fmt"
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

	for s.src.HasNext() {
		t := s.src.Next()
		for _, p := range s.predicates {
			ok, err = p.Eval(t)
			if !ok {
				break
			}
			if err != nil {
				return nil, nil, fmt.Errorf("RelationScanner.Exec: %s(%v) : %w", p, t, err)
			}
		}
		res = append(res, t)
	}

	return s.src.Columns(), res, nil
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
