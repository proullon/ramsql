package engine

import (
	"fmt"
	"strconv"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

// Operator compares 2 values and return a boolean
type Operator func(leftValue Value, rightValue Value) bool

// NewOperator initializes the operator matching the Token number
func NewOperator(token int, lexeme string) (Operator, error) {
	switch token {
	case parser.EqualityToken:
		return equalityOperator, nil
	case parser.LeftDipleToken:
		return lessThanOperator, nil
	case parser.RightDipleToken:
		return greaterThanOperator, nil
	}

	return nil, fmt.Errorf("Operator '%s' does not exist", lexeme)
}

func greaterThanOperator(leftValue Value, rightValue Value) bool {
	log.Critical("LessThanOperator")

	leftv, ok := leftValue.v.(string)
	if !ok {
		log.Critical("GreaterThanOperator: left value value is not a string [%v]", leftValue.v)
		return false
	}

	rightv := rightValue.lexeme
	if rightValue.v != nil {
		rightv, ok = rightValue.v.(string)
		if !ok {
			log.Critical("GreaterThanOperator: right value value is not a string [%v]", rightValue.v)
			return false
		}
	}

	// Let's assume they all are float64
	left, err := strconv.ParseFloat(leftv, 64)
	if err != nil {
		log.Critical("LessThanOperator: %s", err)
	}

	right, err := strconv.ParseFloat(rightv, 64)
	if err != nil {
		log.Critical("LessThanOperator: %s", err)
	}

	return left > right
}

func lessThanOperator(leftValue Value, rightValue Value) bool {
	log.Critical("LessThanOperator")

	leftv, ok := leftValue.v.(string)
	if !ok {
		log.Critical("LessThanOperator: left value value is not a string: %v", leftValue.v)
		return false
	}

	rightv := rightValue.lexeme
	/*if rightValue.v != nil {
		rightv, ok = rightValue.v.(string)
		if !ok {
			log.Critical("LessThanOperator: right value is not a string [%v]", rightValue.v)
			return false
		}
	}*/

	// Let's assume they all are float64
	left, err := strconv.ParseFloat(leftv, 64)
	if err != nil {
		log.Critical("LessThanOperator: %s", err)
	}

	right, err := strconv.ParseFloat(rightv, 64)
	if err != nil {
		log.Critical("LessThanOperator: %s", err)
	}

	return left < right
}

// EqualityOperator checks if given value are equal
func equalityOperator(leftValue Value, rightValue Value) bool {

	if fmt.Sprintf("%v", leftValue.v) == rightValue.lexeme {
		return true
	}

	return false
}

// TrueOperator always returns true
func TrueOperator(leftValue Value, rightValue Value) bool {
	return true
}

func inOperator(leftValue Value, rightValue Value) bool {
	// Right value should be a slice of string
	values, ok := rightValue.v.([]string)
	if !ok {
		log.Debug("InOperator: rightValue.v is not a []string !")
		return false
	}

	for i := range values {
		log.Debug("InOperator: Testing %v against %s", leftValue.v, values[i])
		if fmt.Sprintf("%v", leftValue.v) == values[i] {
			return true
		}
	}

	return false
}
