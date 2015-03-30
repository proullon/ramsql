package engine

import (
	"fmt"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
)

type Operator func(leftValue Value, rightValue Value) bool

func NewOperator(token int, lexeme string) (Operator, error) {
	switch token {
	case parser.EqualityToken:
		return EqualityOperator, nil
	}

	return nil, fmt.Errorf("Operator '%s' does not exist", lexeme)
}

func EqualityOperator(leftValue Value, rightValue Value) bool {
	log.Critical("EqualityOperator!")

	log.Critical("%s vs %s !", leftValue.v, rightValue.lexeme)

	if leftValue.v == rightValue.lexeme {
		log.Critical("%s == %s !", leftValue.v, rightValue.lexeme)
		return true
	}

	log.Critical("RETURNING FALDS")
	return false
}
