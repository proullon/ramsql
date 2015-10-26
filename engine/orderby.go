package engine

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
)

//    |-> order
//        |-> age
//        |-> desc
func orderbyExecutor(attr *parser.Decl, tables []*Table) (selectFunctor, error) {
	f := &orderbyFunctor{}
	f.buffer = make(map[int64][][]string)

	// first subdecl should be attribute
	if len(attr.Decl) < 1 {
		return nil, fmt.Errorf("ordering attribute not provided")
	}

	// FIXME we should find for sure the table of the attribute
	if len(tables) < 1 {
		return nil, fmt.Errorf("cannot guess the table of attribute %s for order", attr.Decl[0].Lexeme)
	}
	f.orderby = tables[0].name + "." + attr.Decl[0].Lexeme

	// if second subdecl is present, it's either asc or desc
	// default is asc anyway
	if len(attr.Decl) == 2 && attr.Decl[1].Token == parser.AscToken {
		f.asc = true
	}

	log.Debug("orderbyExecutor> you must order by '%s', asc: %v\n", f.orderby, f.asc)
	return f, nil
}

// ok so our buffer is a map of INDEX -> slice of ROW
// let's say we can only order by integer values
// and yeah we can have multiple row with one value, order is then random
type orderbyFunctor struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
	orderby    string
	asc        bool
	buffer     map[int64][][]string
}

func (f *orderbyFunctor) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias

	return f.conn.WriteRowHeader(f.alias)
}

func (f *orderbyFunctor) FeedVirtualRow(vrow virtualRow) error {
	var row []string
	var key int64
	var err error

	// search key
	val, ok := vrow[f.orderby]
	if !ok {
		return fmt.Errorf("could not find ordering attribute %s in virtual row", f.orderby)
	}

	key, err = strconv.ParseInt(fmt.Sprintf("%v", val.v), 10, 64)
	if err != nil {
		log.Debug("orderbyFunctor> Cannot parse ordering key %s: %s\n", val, err)
		return fmt.Errorf("error ordering key %s because of value %v", f.orderby, val.v)
	}

	for _, attr := range f.attributes {
		val, ok := vrow[attr]
		if !ok {
			return fmt.Errorf("could not select attribute %s", attr)
		}
		row = append(row, fmt.Sprintf("%v", val.v))
	}

	// now instead of writing row, we will find the ordering key and put in in our buffer
	f.buffer[key] = append(f.buffer[key], row)
	return nil
}

func (f *orderbyFunctor) Done() error {

	// now we have to sort our key
	keys := make([]int64, len(f.buffer))
	var i int64
	for k := range f.buffer {
		keys[i] = k
		i++
	}

	if f.asc {
		sort.Sort(sortSlice(keys))
	} else {
		sort.Sort(sort.Reverse(sortSlice(keys)))
	}

	// now write ordered rows
	for _, key := range keys {
		rows := f.buffer[key]
		for i := range rows {
			f.conn.WriteRow(rows[i])
		}
	}

	return f.conn.WriteRowEnd()
}

type sortSlice []int64

func (s sortSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortSlice) Len() int {
	return len(s)
}
