package engine

import (
	"fmt"
	"github.com/proullon/ramsql/engine/log"
)

// TruePredicate is a predicate wich return always true
var TruePredicate = Predicate{
	True: true,
}

// Value is a value given to predicates
type Value struct {
	v      interface{}
	valid  bool
	lexeme string
}

// Predicate evaluate if a condition is valid with 2 values and an operator on this 2 values
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

// Evaluate calls operators and use tuple as operand
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
