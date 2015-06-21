package ramsql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/go-gorp/gorp"

	"github.com/proullon/ramsql/engine/log"
)

func TestGorp(t *testing.T) {
	log.UseTestLogger(t)

	// initialize the DbMap
	dbmap := initDb(t)
	defer dbmap.Db.Close()

	// delete any existing rows
	err := dbmap.TruncateTables()
	checkErr(t, err, "TruncateTables failed")

	// create two posts
	p1 := newPost("Go 1.1 released!", "Lorem ipsum lorem ipsum")
	p2 := newPost("Go 1.2 released!", "Lorem ipsum lorem ipsum")

	// insert rows - auto increment PKs will be set properly after the insert
	err = dbmap.Insert(&p1, &p2)
	checkErr(t, err, "Insert failed")

	// use convenience SelectInt
	count, err := dbmap.SelectInt("select count(*) from posts")
	checkErr(t, err, "select count(*) failed")
	t.Log("Rows after inserting:", count)

	// update a row
	p2.Title = "Go 1.2 is better than ever"
	count, err = dbmap.Update(&p2)
	checkErr(t, err, "Update failed")
	t.Log("Rows updated:", count)

	// fetch one row - note use of "post_id" instead of "Id" since column is aliased
	//
	// Postgres users should use $1 instead of ? placeholders
	// See 'Known Issues' below
	//
	err = dbmap.SelectOne(&p2, "select * from posts where post_id=?", p2.ID)
	checkErr(t, err, "SelectOne failed")
	t.Log("p2 row:", p2)

	// fetch all rows
	var posts []Post
	_, err = dbmap.Select(&posts, "select * from posts order by post_id")
	checkErr(t, err, "Select failed")
	t.Log("All rows:")
	for x, p := range posts {
		t.Logf("    %d: %v\n", x, p)
	}

	// delete row by PK
	count, err = dbmap.Delete(&p1)
	checkErr(t, err, "Delete failed")
	t.Log("Rows deleted:", count)

	// delete row manually via Exec
	_, err = dbmap.Exec("delete from posts where post_id=?", p2.ID)
	checkErr(t, err, "Exec failed")

	// confirm count is zero
	count, err = dbmap.SelectInt("select count(*) from posts")
	checkErr(t, err, "select count(*) failed")
	t.Log("Row count - should be zero:", count)

	t.Log("Done!")
}

type Post struct {
	// db tag lets you specify the column name if it differs from the struct field
	ID      int64 `db:"post_id"`
	Created int64
	Title   string
	Body    string
}

func newPost(title, body string) Post {
	return Post{
		Created: time.Now().UnixNano(),
		Title:   title,
		Body:    body,
	}
}

func initDb(t *testing.T) *gorp.DbMap {
	// connect to db using standard Go database/sql API
	// use whatever database/sql driver you wish
	db, err := sql.Open("ramsql", "/tmp/post_db.bin")
	if err != nil {
		t.Fatalf("sql.Open failed: %s", err)
	}

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.SqliteDialect{}}

	// add a table, setting the table name to 'posts' and
	// specifying that the Id property is an auto incrementing PK
	dbmap.AddTableWithName(Post{}, "posts").SetKeys(true, "ID")

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	err = dbmap.CreateTablesIfNotExists()
	if err != nil {
		t.Fatalf("Create tables failed: %s", err)
	}

	return dbmap
}

func checkErr(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %s", msg, err)
	}
}
