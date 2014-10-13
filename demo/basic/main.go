package main

import (
	"database/sql"
	"fmt"

	_ "github.com/proullon/ramsql/driver"
)

func main() {

	db, err := sql.Open("ramsql", "")
	if err != nil {
		fmt.Printf("sql.Open : Error : %s\n", err)
		return
	}

	res, err := db.Exec("SELECT * FROM account WHERE email = '?", "foo@bar.com")
	if err != nil {
		fmt.Printf("sql.Exec : Error : %s\n", err)
		return
	}

	i, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("res.RowsAffected : Error : %s\n", err)
		return
	}
	fmt.Printf("%d rows affected\n", i)

	err = db.Close()
	if err != nil {
		fmt.Printf("sql.Close : Error : %s\n", err)
		return
	}
}
