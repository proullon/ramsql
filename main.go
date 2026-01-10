package main

import (
	"database/sql"
	"fmt"

	"github.com/leonardaustin/ramsql/cli"
	_ "github.com/leonardaustin/ramsql/driver"
)

func main() {
	db, err := sql.Open("ramsql", "")
	if err != nil {
		fmt.Printf("Error : cannot open connection : %s\n", err)
		return
	}
	cli.Run(db)
}
