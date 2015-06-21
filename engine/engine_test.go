package engine

import (
	"testing"

	"github.com/proullon/ramsql/engine/protocol"
)

type TestEngineConn struct {
}

func (conn *TestEngineConn) ReadStatement() (string, error) {
	return "", nil
}

func (conn *TestEngineConn) WriteResult(lastInsertedID int64, rowsAffected int64) error {
	return nil
}

func (conn *TestEngineConn) WriteError(err error) error {
	return nil
}

func (conn *TestEngineConn) WriteRowHeader(header []string) error {
	return nil
}

func (conn *TestEngineConn) WriteRow(row []string) error {
	return nil
}

func (conn *TestEngineConn) WriteRowEnd() error {
	return nil
}

func testEngine(t *testing.T) *Engine {
	_, engineEndpoint := protocol.NewChannelEndpoints()
	e, err := New(engineEndpoint)
	if err != nil {
		t.Fatalf("Cannot create new engine: %s", err)
	}

	return e
}

func TestNewEngine(t *testing.T) {
	e := testEngine(t)
	e.Stop()
}
