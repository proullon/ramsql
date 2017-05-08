package engine

import (
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/protocol"
)

type limit struct {
	realConn protocol.EngineConn
	limit    int
	current  int
}

func limitedConn(conn protocol.EngineConn, l int) protocol.EngineConn {
	c := &limit{
		realConn: conn,
		limit:    l,
		current:  0,
	}
	return c
}

// Not needed
func (l *limit) ReadStatement() (string, error) {
	log.Debug("limit.ReadStatement: should not be used\n")
	return "", nil
}

// Not needed
func (l *limit) WriteResult(last int64, ra int64) error {
	log.Debug("limit.WriteResult: should not be used\n")
	return nil
}

func (l *limit) WriteError(err error) error {
	return l.realConn.WriteError(err)
}

func (l *limit) WriteRowHeader(header []string) error {
	return l.realConn.WriteRowHeader(header)
}

func (l *limit) WriteRow(row []string) error {
	if l.current == l.limit {
		// We are done here
		return nil
	}
	l.current++
	return l.realConn.WriteRow(row)
}

func (l *limit) WriteRowEnd() error {
	return l.realConn.WriteRowEnd()
}

type offset struct {
	realConn protocol.EngineConn
	offset   int
	current  int
}

func offsetedConn(conn protocol.EngineConn, o int) protocol.EngineConn {
	c := &offset{
		realConn: conn,
		offset:   o,
	}
	return c
}

// Not needed
func (l *offset) ReadStatement() (string, error) {
	log.Debug("limit.ReadStatement: should not be used\n")
	return "", nil
}

// Not needed
func (l *offset) WriteResult(last int64, ra int64) error {
	log.Debug("limit.WriteResult: should not be used\n")
	return nil
}

func (l *offset) WriteError(err error) error {
	return l.realConn.WriteError(err)
}

func (l *offset) WriteRowHeader(header []string) error {
	return l.realConn.WriteRowHeader(header)
}

func (l *offset) WriteRow(row []string) error {
	if l.current < l.offset {
		// skip this line
		l.current++
		return nil
	}

	return l.realConn.WriteRow(row)
}

func (l *offset) WriteRowEnd() error {
	return l.realConn.WriteRowEnd()
}
