package ramsql

// import (
//        "testing"
//        "database/sql"
// )

// func TestSelect(t *testing.T) {
// 	db, err := sql.Open("ramsql", "")
// 	if err != nil {
// 		t.Fatalf("sql.Open : Error : %s\n", err)
// 	}
// 	defer db.Close()

// 	_, err = db.Exec("CREATE TABLE account (id INT, email TEXT)")
// 	if err != nil {
// 		t.Fatalf("sql.Exec: Error: %s\n", err)
// 	}

// 	rows, err := db.Query("SELECT * FROM account WHERE email = '?'", "foo@bar.com")
// 	if err != nil {
// 		t.Fatalf("sql.Query error : %s\n", err)
// 	}

// 	i := 0
// 	for rows.Next() {
// 	    i++
// 	}
// 	t.Logf("%d rows affected\n", i)

// 	err = db.Close()
// 	if err != nil {
// 		t.Fatalf("sql.Close : Error : %s\n", err)
// 	}
// }
