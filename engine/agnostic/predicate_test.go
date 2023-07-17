package agnostic

import (
	"testing"
)

func checkEval(t *testing.T, p Predicate, tup *Tuple, expect bool) {
	b, err := p.Eval(tup)
	if err != nil {
		t.Fatalf("unexpected error on predicate eval(%v): %s", tup, err)
	}
	if b != expect {
		t.Fatalf("expected %v on predicate eval(%v): %s", expect, tup, err)
	}
}

func TestTruePredicate(t *testing.T) {
	tup := &Tuple{}
	p := NewTruePredicate()
	checkEval(t, p, tup, true)
}

func TestAndPredicate(t *testing.T) {
	tup := &Tuple{}
	p1 := NewTruePredicate()
	p2 := NewTruePredicate()
	p3 := NewFalsePredicate()
	p4 := NewFalsePredicate()

	p := NewAndPredicate(p1, p2)
	checkEval(t, p, tup, true)

	p = NewAndPredicate(p3, p4)
	checkEval(t, p, tup, false)

	p = NewAndPredicate(p1, p4)
	checkEval(t, p, tup, false)

	p = NewAndPredicate(p3, p2)
	checkEval(t, p, tup, false)
}
