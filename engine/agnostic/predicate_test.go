package agnostic

import (
	"testing"
)

func checkEval(t *testing.T, p Predicate, cols []string, tup *Tuple, expect bool) {
	b, err := p.Eval(cols, tup)
	if err != nil {
		t.Fatalf("unexpected error on predicate %s with tuple %v: %s", p, tup, err)
	}
	if b != expect {
		t.Fatalf("expected %v on predicate %s with tuple %v", expect, p, tup)
	}
}

func TestTruePredicate(t *testing.T) {
	tup := &Tuple{}
	p := NewTruePredicate()
	checkEval(t, p, nil, tup, true)
}

func TestAndPredicate(t *testing.T) {
	tup := &Tuple{}
	p1 := NewTruePredicate()
	p2 := NewTruePredicate()
	p3 := NewFalsePredicate()
	p4 := NewFalsePredicate()

	p := NewAndPredicate(p1, p2)
	checkEval(t, p, nil, tup, true)

	p = NewAndPredicate(p3, p4)
	checkEval(t, p, nil, tup, false)

	p = NewAndPredicate(p1, p4)
	checkEval(t, p, nil, tup, false)

	p = NewAndPredicate(p3, p2)
	checkEval(t, p, nil, tup, false)
}

func TestOrPredicate(t *testing.T) {
	tup := &Tuple{}
	p1 := NewTruePredicate()
	p2 := NewTruePredicate()
	p3 := NewFalsePredicate()
	p4 := NewFalsePredicate()

	p := NewOrPredicate(p1, p2)
	checkEval(t, p, nil, tup, true)

	p = NewOrPredicate(p3, p4)
	checkEval(t, p, nil, tup, false)

	p = NewOrPredicate(p1, p4)
	checkEval(t, p, nil, tup, true)

	p = NewOrPredicate(p3, p2)
	checkEval(t, p, nil, tup, true)
}

func TestEqPredicate(t *testing.T) {
	rname := "pfwefwpfw"
	tup := NewTuple("abra", "cada", "bra")
	cols := []string{"a", "b", "c"}

	c1 := NewConstValueFunctor(12)
	c2 := NewConstValueFunctor(12)
	c3 := NewConstValueFunctor(13)
	c4 := NewConstValueFunctor("g430f09jf20jf23")
	c5 := NewConstValueFunctor("cada")

	a1 := NewAttributeValueFunctor(rname, "a")
	b1 := NewAttributeValueFunctor(rname, "b")

	p := NewEqPredicate(c1, c2)
	checkEval(t, p, cols, tup, true)

	p = NewEqPredicate(c1, c3)
	checkEval(t, p, cols, tup, false)

	p = NewEqPredicate(a1, c1)
	checkEval(t, p, cols, tup, false)

	p = NewEqPredicate(a1, b1)
	checkEval(t, p, cols, tup, false)

	p = NewEqPredicate(a1, c4)
	checkEval(t, p, cols, tup, false)

	p = NewEqPredicate(a1, c5)
	checkEval(t, p, cols, tup, false)

	p = NewEqPredicate(b1, c5)
	checkEval(t, p, cols, tup, true)

	p = NewEqPredicate(b1, c4)
	checkEval(t, p, cols, tup, false)
}
