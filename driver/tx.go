package ramsql

//
// Tx doesn't need to be in driver package.
//
// Implementation doesn't depend on any sql/driver type, and can live in executor package.
//

/*
import (
	"context"
	"database/sql/driver"

	"github.com/proullon/ramsql/engine/agnostic"
	"github.com/proullon/ramsql/engine/executor"
)

// Tx implements SQL transaction method
type Tx struct {
	e  *executor.Engine
	tx *agnostic.Transaction
}

*/
