package engine

import (
	"fmt"
	"github.com/proullon/ramsql/engine/log"
	"github.com/proullon/ramsql/engine/parser"
	"github.com/proullon/ramsql/engine/protocol"
	"sort"
)

//    |-> order
//        |-> age
//        |-> desc
func orderbyExecutor(attr *parser.Decl, tables []*Table) (selectFunctor, error) {
	f := &orderbyFunctor{}

	// first subdecl should be attribute
	if len(attr.Decl) < 1 {
		return nil, fmt.Errorf("ordering attribute not provided")
	}

	// FIXME we should find for sure the table of the attribute
	if len(tables) < 1 {
		return nil, fmt.Errorf("cannot guess the table of attribute %s for order", attr.Decl[0].Lexeme)
	}

	for _, d := range attr.Decl {
		var desc bool
		if len(d.Decl) >= 1 && d.Decl[0].Lexeme == "desc" {
			desc = true
		}

		f.orderBy = append(f.orderBy, &orderBy{
			column: tables[0].name + "." + d.Lexeme,
			desc:   desc,
		})
	}

	return f, nil
}

type orderBy struct {
	column     string
	desc       bool
	comparator comparator
}

// ok so our buffer is a map of INDEX -> slice of ROW
// let's say we can only order by integer values
// and yeah we can have multiple row with one value, order is then random
type orderbyFunctor struct {
	e          *Engine
	conn       protocol.EngineConn
	attributes []string
	alias      []string
	order      orderer
	orderBy    []*orderBy
}

func (f *orderbyFunctor) Init(e *Engine, conn protocol.EngineConn, attr []string, alias []string) error {
	f.e = e
	f.conn = conn
	f.attributes = attr
	f.alias = alias

	return f.conn.WriteRowHeader(f.alias)
}

func (f *orderbyFunctor) FeedVirtualRow(vrow virtualRow) error {
	if f.order == nil { // first time
		o := &genericOrderer{orderBy: f.orderBy}
		o.init(f.attributes)

		f.order = o
	}

	return f.order.Feed(Value{}, vrow)
}

func (f *orderbyFunctor) Done() error {
	log.Debug("orderByFunctor.Done\n")

	// No row in result set, orderer hasn't been initialized
	if f.order == nil {
		return f.conn.WriteRowEnd()
	}

	if err := f.order.Sort(); err != nil {
		return err
	}

	err := f.order.Write(f.conn)
	if err != nil {
		return err
	}

	return f.conn.WriteRowEnd()
}

type orderer interface {
	Feed(key Value, vrow virtualRow) error
	Sort() error
	Write(conn protocol.EngineConn) error
}

type genericOrderer struct {
	buffer     map[string][][]interface{}
	attributes []string
	keys       []string
	orderBy    []*orderBy
}

func (o *genericOrderer) init(attr []string) {
	o.buffer = make(map[string][][]interface{})
	o.attributes = attr
}

func (o *genericOrderer) Feed(_ Value, vrow virtualRow) error {
	var row []interface{}

	var key string
	for _, ob := range o.orderBy {
		if key != "" {
			key += "."
		}

		key += fmt.Sprint(vrow[ob.column].v)

		if ob.comparator != nil {
			continue
		}

		// TODO: refactor using generics once possible
		switch vrow[ob.column].v.(type) {
		case string:
			ob.comparator = func(desc bool) comparator {
				return func(i interface{}, j interface{}) int {
					if _, ok := i.(string); !ok {
						panic(fmt.Sprintf("unsupported type, expected string but got %T", i))
					}

					a := i.(string)
					b := j.(string)
					if desc {
						switch {
						case a > b:
							return 1
						case a < b:
							return -1
						case a == b:
							return 0
						}
					} else {
						switch {
						case a < b:
							return 1
						case a > b:
							return -1
						case a == b:
							return 0
						}
					}
					return 0
				}
			}(ob.desc)
		case int64:
			ob.comparator = func(desc bool) comparator {
				return func(i interface{}, j interface{}) int {
					if _, ok := i.(int64); !ok {
						panic(fmt.Sprintf("unsupported type, expected int64 but got %T %v", i, i))
					}

					a := i.(int64)
					b := j.(int64)
					if desc {
						switch {
						case a > b:
							return 1
						case a < b:
							return -1
						case a == b:
							return 0
						}
					} else {
						switch {
						case a < b:
							return 1
						case a > b:
							return -1
						case a == b:
							return 0
						}
					}
					return 0
				}
			}(ob.desc)
		case float64:
			ob.comparator = func(desc bool) comparator {
				return func(i interface{}, j interface{}) int {
					if _, ok := i.(float64); !ok {
						panic(fmt.Sprintf("unsupported type, expected float64 but got %T %v", i, i))
					}

					a := i.(float64)
					b := j.(float64)
					if desc {
						switch {
						case a > b:
							return 1
						case a < b:
							return -1
						case a == b:
							return 0
						}
					} else {
						switch {
						case a < b:
							return 1
						case a > b:
							return -1
						case a == b:
							return 0
						}
					}
					return 0
				}
			}(ob.desc)
		//default:
		//	panic(fmt.Sprintf("wrong type %T for column %s", vrow[ob.column].v, ob.column))
		}
	}

	for _, attr := range o.attributes {
		val, ok := vrow[attr]
		if !ok {
			return fmt.Errorf("could not select attribute %s", attr)
		}
		row = append(row, val.v)
	}

	// now instead of writing row, we will find the ordering key and put in in our buffer
	o.buffer[key] = append(o.buffer[key], row)
	return nil
}

func (o *genericOrderer) Sort() error {
	o.keys = make([]string, len(o.buffer))
	var idx int
	for k := range o.buffer {
		o.keys[idx] = k
		idx++
	}

	sort.Slice(o.keys, func(a, b int) bool {
		for _, ob := range o.orderBy {
			keyA := o.keys[a]
			keyB := o.keys[b]

			var idx int
			for i, attr := range o.attributes {
				if attr == ob.column {
					idx = i
				}
			}

			switch ob.comparator(o.buffer[keyA][0][idx], o.buffer[keyB][0][idx]) {
			case 1:
				return true
			case 0:
				continue
			case -1:
				return false
			}
		}

		return false
	})

	return nil
}

func (o *genericOrderer) Write(conn protocol.EngineConn) error {
	// now write ordered rows
	for _, key := range o.keys {
		rows := o.buffer[key]
		for index := range rows {
			var row []string
			for _, elem := range rows[index] {
				row = append(row, fmt.Sprint(elem))
			}
			err := conn.WriteRow(row)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type comparator func(i interface{}, j interface{}) int
