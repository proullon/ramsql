package ramsql

import (
	"errors"
)

type RamSQLError string

var (
	NotImplemented RamSQLError = "Not implemented, haha"
)

func newError(err RamSQLError) error {
	return errors.New(string(err))
}
