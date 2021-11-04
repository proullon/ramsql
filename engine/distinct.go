package engine

import (
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/protocol"
)

type distinct struct {
	realConn protocol.EngineConn
	seen     seen
	len      int
}

func distinctedConn(conn protocol.EngineConn, len int) protocol.EngineConn {
	return &distinct{
		realConn: conn,
		len:      len,
		seen:     make(seen),
	}
}

// Not needed
func (l *distinct) ReadStatement() (string, error) {
	log.Debug("limit.ReadStatement: should not be used\n")
	return "", nil
}

// Not needed
func (l *distinct) WriteResult(last int64, ra int64) error {
	log.Debug("limit.WriteResult: should not be used\n")
	return nil
}

func (l *distinct) WriteError(err error) error {
	return l.realConn.WriteError(err)
}

func (l *distinct) WriteRowHeader(header []string) error {
	if l.len > 0 {
		// Postgres returns only columns outside of DISTINCT ON
		return l.realConn.WriteRowHeader(header[l.len:])
	}
	return l.realConn.WriteRowHeader(header)
}

func (l *distinct) WriteRow(row []string) error {
	if l.len > 0 {
		if l.seen.exists(row[:l.len]) {
			return nil
		}
		// Postgres returns only columns outside of DISTINCT ON
		return l.realConn.WriteRow(row[l.len:])
	} else {
		if l.seen.exists(row) {
			return nil
		}
		return l.realConn.WriteRow(row)
	}
}

func (l *distinct) WriteRowEnd() error {
	return l.realConn.WriteRowEnd()
}

func (l *distinct) equalRows(a, b []string) bool {
	if l.len > 0 {
		if len(a) < l.len || len(b) < l.len {
			return false
		}

		for idx := 0; idx < l.len; idx++ {
			if a[idx] != b[idx] {
				return false
			}
		}

		return true
	}

	if len(a) != len(b) {
		return false
	}
	for idx := range a {
		if a[idx] != b[idx] {
			return false
		}
	}

	return true
}

type seen map[string]seen

func (s seen) exists(r []string) bool {
	if c, ok := s[r[0]]; ok {
		if len(r) == 1 {
			return true
		}

		return c.exists(r[1:])
	}

	s[r[0]] = make(seen)
	if len(r) == 1 {
		return false
	}

	// does not exists, but we want to populate the tree fully
	return s[r[0]].exists(r[1:])
}
