package engine

import (
	"fmt"
	"github.com/proullon/ramsql/engine/log"
)

var TruePredicate = Predicate{
	True: true,
}

type Value struct {
	v      interface{}
	valid  bool
	lexeme string
}

type Predicate struct {
	LeftValue  Value
	Operator   Operator
	RightValue Value
	True       bool
}

func NewPredicate() *Predicate {
	return nil
}

func (p Predicate) String() string {
	var left, right string

	left = "?"
	right = "?"

	if p.LeftValue.valid {
		left = p.LeftValue.lexeme
	}

	if p.RightValue.valid {
		right = p.RightValue.lexeme
	}

	return fmt.Sprintf("[%s] vs [%s]", left, right)
}

func (p *Predicate) Evaluate(t *Tuple, table *Table) bool {
	log.Debug("Evaluating predicate %s", p)

	if p.True {
		return true
	}

	// Find left
	var i = 0
	lenTable := len(table.attributes)
	for i = 0; i < lenTable; i++ {
		if table.attributes[i].name == p.LeftValue.lexeme {
			break
		}
	}
	if i == lenTable {
		panic(p.LeftValue.lexeme + "not found !")
	}

	p.LeftValue.v = t.Values[i]
	return p.Operator(p.LeftValue, p.RightValue)
}
