package ramsql

import (
	"fmt"
)

// Tx implements SQL transaction method
type Tx struct {
}

// Commit the transaction on server
func (t *Tx) Commit() error {
	return fmt.Errorf("Not implemented")
}

// Rollback all changes
func (t *Tx) Rollback() error {
	return fmt.Errorf("Not implemented")
}
