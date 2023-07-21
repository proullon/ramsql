package agnostic

import (
	"reflect"
	"testing"
	"time"

	"github.com/proullon/ramsql/engine/log"
)

func TestTransactionEmptyCommit(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 0 {
		t.Fatalf("expected no line changed, got %d", changed)
	}
}

func TestTransactionEmptyRollback(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}

	tx.Rollback()
	tx.Rollback()
}

func TestCreateRelation(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		Attribute{
			name:     "foo",
			typeName: "BIGINT",
		},
		Attribute{
			name:     "bar",
			typeName: "TEXT",
		},
	}

	err = tx.CreateRelation("", "myrel", attrs, nil)
	if err != nil {
		t.Fatalf("cannot create table: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	if len(e.schemas[DefaultSchema].relations) != 1 {
		t.Fatalf("expected a relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	tx.Rollback()
}

func TestDropRelation(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		Attribute{
			name:     "foo",
			typeName: "BIGINT",
		},
		Attribute{
			name:     "bar",
			typeName: "TEXT",
		},
	}

	err = tx.CreateRelation("", "myrel", attrs, nil)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	err = tx.DropRelation("", "myrel")
	if err != nil {
		t.Fatalf("cannot drop relation: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 2 {
		t.Fatalf("expected 2 change, got %d", changed)
	}

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}
}

func TestInsertTotal(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("foo", "BIGINT"),
		NewAttribute("bar", "TEXT"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs, nil)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["bar"] = "test"
	tuple, err := tx.Insert(schema, relation, values)
	if err == nil {
		t.Fatalf("expected error with foo attribute not specified")
	}

	tuple, err = tx.Insert(schema, relation, values)
	if err == nil {
		t.Fatalf("expected transaction aborted due to previous error")
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values["foo"] = 1
	tuple, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	l := len(tuple.values)
	if l != 2 {
		t.Fatalf("expected 2 values in tuple, got %d", l)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	l = e.schemas[schema].relations[relation].rows.Len()
	if l != 1 {
		t.Fatalf("expected 1 rows in relation, got %d", l)
	}
}

func TestInsertRollback(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("foo", "BIGINT"),
		NewAttribute("bar", "TEXT"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs, nil)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["bar"] = "test"
	values["foo"] = 1
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	tx.Rollback()

	l := e.schemas[schema].relations[relation].rows.Len()
	if l != 0 {
		t.Fatalf("expected 0 rows in relation, got %d", l)
	}
}

func TestInsertPartial(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("default_answer", "INT").WithDefaultConst(42),
		NewAttribute("foo", "JSON"),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs, nil)
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit transaction: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	values := make(map[string]any)
	values["foo"] = `{}`
	tuple, err := tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	l := len(tuple.values)
	if l != 3 {
		t.Fatalf("expected 3 values in tuple, got %d", l)
	}
	if _, ok := tuple.values[0].(int64); !ok {
		t.Fatalf("expected 1st tuple value to be an int64, got %s", reflect.TypeOf(tuple.values[0]))
	}
	if val, _ := tuple.values[0].(int64); val != 1 {
		t.Fatalf("expected 1st tuple value to be 1, got %d", val)
	}
	if _, ok := tuple.values[1].(int64); !ok {
		t.Fatalf("expected 2nd tuple value to be an int64, got %s", reflect.TypeOf(tuple.values[1]))
	}
	if val, _ := tuple.values[1].(int64); val != 42 {
		t.Fatalf("expected 2nd tuple value to be 1, got %d", val)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 1 {
		t.Fatalf("expected 1 change, got %d", changed)
	}

	l = e.schemas[schema].relations[relation].rows.Len()
	if l != 1 {
		t.Fatalf("expected 1 rows in relation, got %d", l)
	}
}

func TestIndexCreation(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("default_answer", "INT").WithDefaultConst(42),
		NewAttribute("foo", "JSON").WithUnique(),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs, []string{"id"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	values := make(map[string]any)
	values["foo"] = `{"foo":"a"}`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	values["foo"] = `{"foo":"b"}`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	values["foo"] = `{"foo":"c"}`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	changed, err := tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}
	if changed != 4 {
		t.Fatalf("expected 4 change, got %d", changed)
	}

	l := e.schemas[schema].relations[relation].rows.Len()
	if l != 3 {
		t.Fatalf("expected 3 rows in relation, got %d", l)
	}

	l = len(e.schemas[schema].relations[relation].indexes)
	if l != 2 {
		t.Fatalf("expected 2 indexes for relation, got %d", l)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	_, err = tx.Insert(schema, relation, values)
	if err == nil {
		t.Fatalf("expected UNIQUE violation on json")
	}
}

func TestQuery(t *testing.T) {
	e := NewEngine()
	log.SetLevel(log.InfoLevel)

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	if len(e.schemas[DefaultSchema].relations) != 0 {
		t.Fatalf("expected 0 relation in default schema, got %d", len(e.schemas[DefaultSchema].relations))
	}

	attrs := []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("default_answer", "INT").WithDefaultConst(42),
		NewAttribute("foo", "TEXT").WithUnique(),
	}

	schema := DefaultSchema
	relation := "myrel"

	err = tx.CreateRelation(schema, relation, attrs, []string{"id"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	values := make(map[string]any)
	values["foo"] = `a`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	values["foo"] = `b`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	values["foo"] = `c`
	_, err = tx.Insert(schema, relation, values)
	if err != nil {
		t.Fatalf("cannot insert values: %s", err)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}

	tx, err = e.Begin()
	if err != nil {
		t.Fatalf("cannot begin 2nd tx: %s", err)
	}
	defer tx.Rollback()

	schema = DefaultSchema
	relation = "task"
	attrs = []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("val", "INT").WithDefaultConst(42),
		NewAttribute("name", "TEXT").WithUnique().WithDefault(NewRandString(20)),
	}
	err = tx.CreateRelation(schema, relation, attrs, []string{"id"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	schema = DefaultSchema
	relation = "task_link"
	attrs = []Attribute{
		NewAttribute("parent_id", "BIGINT"),
		NewAttribute("child_id", "BIGINT"),
	}
	err = tx.CreateRelation(schema, relation, attrs, []string{"parent_id", "child_id"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	relation = "myrel"
	columns, tuples, err := tx.Query(DefaultSchema, []Selector{&StarSelector{relation: relation}}, NewTruePredicate(), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on Query: %s", err)
	}

	l := len(columns)
	if l != 3 {
		t.Fatalf("expected 3 columns in query return, got %d", l)
	}

	l = len(tuples)
	if l != 3 {
		t.Fatalf("expected 3 tuples in query result, got %d", l)
	}

	columns, tuples, err = tx.Query(
		DefaultSchema,
		[]Selector{
			NewAttributeSelector("task", []string{"id", "val", "name"}),
			NewAttributeSelector("task_link", []string{"child_id"}),
		},
		NewEqPredicate(
			NewAttributeValueFunctor("task", "id"),
			NewConstValueFunctor(23),
		),
		[]Joiner{
			NewNaturalJoin("task", "id", "task_link", "parent_id"),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error on Query: %s", err)
	}

	l = len(columns)
	if l != 4 {
		t.Fatalf("expected 4 columns in query return, got %d", l)
	}

	l = len(tuples)
	if l != 0 {
		t.Fatalf("expected 3 tuples in query result, got %d", l)
	}

	for i := 0; i < 100; i++ {
		_, err = tx.Insert(schema, "task", values)
		if err != nil {
			t.Fatalf("cannot insert values: %s", err)
		}

		if i == 0 {
			continue
		}

		for j := 50; j < 100; j++ {
			values["parent_id"] = i
			values["child_id"] = j
			_, err = tx.Insert(schema, "task_link", values)
			if err != nil {
				t.Fatalf("cannot insert values: %s", err)
			}
		}
	}

	columns, tuples, err = tx.Query(
		DefaultSchema,
		[]Selector{
			NewAttributeSelector("task", []string{"id", "val", "name"}),
			NewAttributeSelector("task_link", []string{"child_id"}),
		},
		NewEqPredicate(
			NewAttributeValueFunctor("task", "id"),
			NewConstValueFunctor(23),
		),
		[]Joiner{
			NewNaturalJoin("task", "id", "task_link", "parent_id"),
		},
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error on Query: %s", err)
	}

	l = len(columns)
	if l != 4 {
		t.Fatalf("expected 4 columns in query return, got %d", l)
	}

	l = len(tuples)
	if l != 50 {
		t.Fatalf("expected 50 tuples in query result, got %d", l)
	}

	_, err = tx.Commit()
	if err != nil {
		t.Fatalf("cannot commit tx: %s", err)
	}

}

func TestCount(t *testing.T) {
	e := NewEngine()
	log.SetLevel(log.InfoLevel)

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	schema := DefaultSchema
	relation := "task"
	attrs := []Attribute{
		NewAttribute("id", "BIGINT").WithAutoIncrement(),
		NewAttribute("val", "INT").WithDefaultConst(42),
		NewAttribute("name", "TEXT").WithUnique().WithDefault(NewRandString(20)),
	}
	err = tx.CreateRelation(schema, relation, attrs, []string{"id"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	values := make(map[string]any)
	for i := 0; i < 100; i++ {
		_, err = tx.Insert(schema, "task", values)
		if err != nil {
			t.Fatalf("cannot insert values: %s", err)
		}
	}

	columns, tuples, err := tx.Query(
		DefaultSchema,
		[]Selector{
			NewCountSelector("task", "id"),
		},
		NewTruePredicate(),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error on Query: %s", err)
	}

	l := len(columns)
	if l != 1 {
		t.Fatalf("expected 1 column, got %d", l)
	}
	if columns[0] != "COUNT(id)" {
		t.Fatalf("unexpected column name: got %s", columns[0])
	}

	l = len(tuples)
	if l != 1 {
		t.Fatalf("expected 1 tuple, got %d", l)
	}
	count := tuples[0].values[0].(int64)
	if count != 100 {
		t.Fatalf("expected count to be 100, got %d", count)
	}

	columns, tuples, err = tx.Query(
		DefaultSchema,
		[]Selector{
			NewCountSelector("task", "id"),
		},
		NewEqPredicate(
			NewAttributeValueFunctor("task", "id"),
			NewConstValueFunctor(23),
		),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error on Query: %s", err)
	}

	l = len(columns)
	if l != 1 {
		t.Fatalf("expected 1 column, got %d", l)
	}
	if columns[0] != "COUNT(id)" {
		t.Fatalf("unexpected column name: got %s", columns[0])
	}

	l = len(tuples)
	if l != 1 {
		t.Fatalf("expected 1 tuple, got %d", l)
	}
	count = tuples[0].values[0].(int64)
	if count != 1 {
		t.Fatalf("expected count to be 1, got %d", count)
	}
}

// From Postgres documentation
//
// SELECT DISTINCT ON (location) location, time, report
//
//	FROM weather_reports
//	ORDER BY location, time DESC;
func TestDistinct(t *testing.T) {
	e := NewEngine()

	tx, err := e.Begin()
	if err != nil {
		t.Fatalf("cannot begin tx: %s", err)
	}
	defer tx.Rollback()

	schema := DefaultSchema
	relation := "weather_reports"
	attrs := []Attribute{
		NewAttribute("location", "TEXT"),
		NewAttribute("time", "TIMESTAMP").WithDefaultNow(),
		NewAttribute("report", "TEXT").WithDefault(NewRandString(200)),
	}
	err = tx.CreateRelation(schema, relation, attrs, []string{"location", "time"})
	if err != nil {
		t.Fatalf("cannot create relation: %s", err)
	}

	sometime := time.Date(2023, 07, 21, 0, 0, 0, 0, time.FixedZone("UTC-8", 0))
	somewhere := []string{"Toronto", "Quebec", "Atlanta", "Montreal", "Vancouver"}
	values := make(map[string]any)
	for _, loc := range somewhere {
		thatTimeOfYear := sometime
		for i := 0; i < 10; i++ {
			values["location"] = loc
			values["time"] = thatTimeOfYear
			_, err = tx.Insert(schema, relation, values)
			if err != nil {
				t.Fatalf("cannot insert values: %s", err)
			}
			thatTimeOfYear = thatTimeOfYear.Add(24 * time.Hour * 7)
		}
	}

	cols, res, err := tx.Query(
		DefaultSchema,
		[]Selector{
			NewAttributeSelector(relation, []string{"location", "time", "report"}),
		},
		NewTruePredicate(),
		nil,
		[]Sorter{
			NewDistinctSorter(relation, []string{"location"}),
			NewOrderByDescSorter(relation, []string{"location", "time"}),
		},
	)
	if err != nil {
		t.Fatalf("cannot execure query: %s", err)
	}
	l := len(cols)
	if l != 3 {
		t.Fatalf("expected 3 columns, got %d", l)
	}
	l = len(res)
	if l != 5 {
		t.Fatalf("expected 5 rows, got %d", l)
	}
	sometime = sometime.Add(9 * 24 * time.Hour * 7)
	for _, tuple := range res {
		date := tuple.values[1].(time.Time)
		if date.Unix() != sometime.Unix() {
			t.Fatalf("expected order by time DESC, got %s instead of %s", date, sometime)
		}
	}
}
