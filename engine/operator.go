package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
)

// Operator compares 2 values and return a boolean
type Operator func(leftValue Value, rightValue Value) bool

// NewOperator initializes the operator matching the Token number
func NewOperator(token int, lexeme string) (Operator, error) {
	switch token {
	case parser.EqualityToken:
		return EqualityOperator, nil
	}

	return nil, fmt.Errorf("Operator '%s' does not exist", lexeme)
}

// EqualityOperator checks if given value are equal
func EqualityOperator(leftValue Value, rightValue Value) bool {

	if fmt.Sprintf("%v", leftValue.v) == rightValue.lexeme {
		return true
	}

	return false
}

// TrueOperator always returns true
func TrueOperator(leftValue Value, rightValue Value) bool {
	return true
}
