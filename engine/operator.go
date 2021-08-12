package engine

import (
	"fmt"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"strconv"
	"time"
)

// Operator compares 2 values and return a boolean
type Operator func(leftValue Value, rightValue Value) bool

// NewOperator initializes the operator matching the Token number
func NewOperator(token int, lexeme string) (Operator, error) {
	switch token {
	case parser.EqualityToken:
		return equalityOperator, nil
	case parser.DistinctnessToken:
		return distinctnessOperator, nil
	case parser.LeftDipleToken:
		return lessThanOperator, nil
	case parser.RightDipleToken:
		return greaterThanOperator, nil
	case parser.LessOrEqualToken:
		return lessOrEqualOperator, nil
	case parser.GreaterOrEqualToken:
		return greaterOrEqualOperator, nil
	}

	return nil, fmt.Errorf("Operator '%s' does not exist", lexeme)
}

func convToDate(t interface{}) (time.Time, error) {

	switch t := t.(type) {
	default:
		log.Debug("convToDate> unexpected type %T\n", t)
		return time.Time{}, fmt.Errorf("unexpected internal type %T", t)
	case string:
		d, err := parser.ParseDate(string(t))
		if err != nil {
			return time.Time{}, fmt.Errorf("cannot parse date %v", t)
		}

		return *d, nil
	}

}

func convToFloat(t interface{}) (float64, error) {

	switch t := t.(type) {
	default:
		log.Debug("convToFloat> unexpected type %T\n", t)
		return 0, fmt.Errorf("unexpected internal type %T", t)
	case float64:
		return float64(t), nil
	case int64:
		return float64(int64(t)), nil
	case int:
		return float64(int(t)), nil
	case string:
		return strconv.ParseFloat(string(t), 64)
	}

}

func greaterThanOperator(leftValue Value, rightValue Value) bool {
	log.Debug("GreaterThanOperator")
	var left, right float64
	var err error

	var rvalue interface{}
	if rightValue.v != nil {
		rvalue = rightValue.v
	} else {
		rvalue = rightValue.lexeme
	}

	var leftDate time.Time
	var isDate bool

	left, err = convToFloat(leftValue.v)
	if err != nil {
		leftDate, err = convToDate(leftValue.v)
		if err != nil {
			log.Debug("GreaterThanOperator> %s\n", err)
			return false
		}
		isDate = true
	}

	if !isDate {
		right, err = convToFloat(rvalue)
		if err != nil {
			log.Debug("GreaterThanOperator> %s\n", err)
			return false
		}

		return left > right
	}

	rightDate, err := convToDate(rvalue)
	if err != nil {
		log.Debug("GreaterThanOperator> %s\n", err)
		return false
	}

	return leftDate.After(rightDate)
}

func lessOrEqualOperator(leftValue Value, rightValue Value) bool {
	return lessThanOperator(leftValue, rightValue) || equalityOperator(leftValue, rightValue)
}

func greaterOrEqualOperator(leftValue Value, rightValue Value) bool {
	return greaterThanOperator(leftValue, rightValue) || equalityOperator(leftValue, rightValue)
}

func lessThanOperator(leftValue Value, rightValue Value) bool {
	log.Debug("LessThanOperator")
	var left, right float64
	var err error

	var rvalue interface{}
	if rightValue.v != nil {
		rvalue = rightValue.v
	} else {
		rvalue = rightValue.lexeme
	}

	var leftDate time.Time
	var isDate bool

	left, err = convToFloat(leftValue.v)
	if err != nil {
		leftDate, err = convToDate(leftValue.v)
		if err != nil {
			log.Debug("LessThanOperator> %s\n", err)
			return false
		}
		isDate = true
	}

	if !isDate {
		right, err = convToFloat(rvalue)
		if err != nil {
			log.Debug("LessThanOperator> %s\n", err)
			return false
		}

		return left < right
	}

	rightDate, err := convToDate(rvalue)
	if err != nil {
		log.Debug("LessThanOperator> %s\n", err)
		return false
	}

	return leftDate.Before(rightDate)
}

// EqualityOperator checks if given value are equal
func equalityOperator(leftValue Value, rightValue Value) bool {

	if fmt.Sprintf("%v", leftValue.v) == rightValue.lexeme {
		return true
	}

	return false
}

// DistinctnessOperator checks if given value are distinct
func distinctnessOperator(leftValue Value, rightValue Value) bool {

	if fmt.Sprintf("%v", leftValue.v) != rightValue.lexeme {
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

func notInOperator(leftValue Value, rightValue Value) bool {
	return !inOperator(leftValue, rightValue)
}

func isNullOperator(leftValue Value, rightValue Value) bool {
	return leftValue.v == nil
}

func isNotNullOperator(leftValue Value, rightValue Value) bool {
	return leftValue.v != nil
}
