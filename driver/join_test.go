package ramsql

import (
	"database/sql"
	"testing"

	"github.com/go-gorp/gorp"

	"github.com/proullon/ramsql/engine/log"
)

type User struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type Project struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

type UserProject struct {
	UserID    int64 `db:"user_id"`
	ProjectID int64 `db:"project_id"`
}

func TestJoin(t *testing.T) {
	log.UseTestLogger(t)

	db, err := sql.Open("ramsql", "TestJoin")
	if err != nil {
		t.Fatalf("sql.Open failed: %s", err)
	}
	defer db.Close()

	// construct a gorp DbMap
	dbmap := &gorp.DbMap{Db: db, Dialect: gorp.PostgresDialect{}}

	// add a table, setting the table name to 'posts' and
	// specifying that the Id property is an auto incrementing PK
	dbmap.AddTableWithName(User{}, "user").SetKeys(true, "id")
	dbmap.AddTableWithName(Project{}, "project").SetKeys(true, "id")
	dbmap.AddTableWithName(UserProject{}, "user_project")

	// create the table. in a production system you'd generally
	// use a migration tool, or create the tables via scripts
	err = dbmap.CreateTablesIfNotExists()
	if err != nil {
		t.Fatalf("Create tables failed: %s", err)
	}

	foo := User{
		Name: "foo",
	}

	bar := User{
		Name: "bar",
	}

	testProject := Project{
		Name: "Test project",
	}

	foobarInc := Project{
		Name: "FooBar Inc",
	}

	err = dbmap.Insert(&foo)
	if err != nil {
		t.Fatal(err)
	}
	err = dbmap.Insert(&bar)
	if err != nil {
		t.Fatal(err)
	}
	err = dbmap.Insert(&testProject)
	if err != nil {
		t.Fatal(err)
	}
	err = dbmap.Insert(&foobarInc)
	if err != nil {
		t.Fatal(err)
	}

	// Check if insert is ok
	if foo.ID == 0 {
		t.Fatalf("Foo should have an ID: got 0")
	}

	if bar.ID == 0 {
		t.Fatalf("Bar should have an ID: got 0")
	}

	// Add foo to project test
	err = dbmap.Insert(&UserProject{UserID: foo.ID, ProjectID: testProject.ID})
	if err != nil {
		t.Fatal(err)
	}

	err = dbmap.Insert(&UserProject{UserID: foo.ID, ProjectID: foobarInc.ID})
	if err != nil {
		t.Fatal(err)
	}

	err = dbmap.Insert(&UserProject{UserID: bar.ID, ProjectID: foobarInc.ID})
	if err != nil {
		t.Fatal(err)
	}

	// Now select all projects for foo
	var projects []Project
	query := `SELECT * FROM project
						JOIN user_project ON "user_project".project_id = "project".id
						WHERE "user_project".user_id = $1`
	_, err = dbmap.Select(&projects, query, foo.ID)
	if err != nil {
		t.Fatalf("Cannot select user projects: %s", err)
	}

	if len(projects) != 2 {
		t.Fatalf("Expected 2 projects, got %d", len(projects))
	}
}
