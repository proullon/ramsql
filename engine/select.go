package engine

import (
	"fmt"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

type selectFunctor interface {
	Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error
	Feed(t *Tuple) error
	Done() error
}

// getSelectFunctors instanciate new functors for COUNT, MAX, MIN, AVG, ... and default select functor that return rows to client
// If a functor is specified, no attribute can be selected ?
func getSelectFunctors(attr *parser.Decl) ([]selectFunctor, error) {
	log.Debug("getSelectFunctors")
	var functors []selectFunctor

	for i := range attr.Decl {

		if attr.Decl[i].Token == parser.FromToken {
			break
		}

		if attr.Decl[i].Token == parser.CountToken {
			f := &countSelectFunction{}
			functors = append(functors, f)
		}
	}

	if len(functors) == 0 {
		f := &defaultSelectFunction{}
		functors = append(functors, f)
	}

	return functors, nil

}

type defaultSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
}

func (f *defaultSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias

	return f.conn.WriteRowHeader(f.alias)
}

func (f *defaultSelectFunction) Feed(t *Tuple) error {
	var row []string
	for _, value := range t.Values {
		row = append(row, fmt.Sprintf("%v", value))
	}
	log.Debug("Writing row  %v", row)
	return f.conn.WriteRow(row)
}

func (f *defaultSelectFunction) Done() error {
	return f.conn.WriteRowEnd()
}

type countSelectFunction struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
	Count      int64
}

func (f *countSelectFunction) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	log.Debug("countSelectFunction.Init\nReceived attr=%v\nalias=%v\n", attr, alias)
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias
	return nil
}

func (f *countSelectFunction) Feed(t *Tuple) error {
	f.Count++
	log.Critical("countSelectFunction.Feed")
	return nil
}

func (f *countSelectFunction) Done() error {
	log.Debug("-> Writing row header : %v\n", f.alias)
	err := f.conn.WriteRowHeader(f.alias)
	if err != nil {
		return err
	}

	log.Debug("countSelectFunction.Done: Writing %d", f.Count)
	err = f.conn.WriteRow([]string{fmt.Sprintf("%d", f.Count)})
	if err != nil {
		return err
	}

	return f.conn.WriteRowEnd()
}
