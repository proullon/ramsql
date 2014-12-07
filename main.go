package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/proullon/ramsql/engine/log"

	_ "github.com/proullon/ramsql/driver"
)

func init() {
	log.SetLevel(0)
}
func exec(db *sql.DB, stmt string) {

	res, err := db.Exec(stmt)
	if err != nil {
		fmt.Printf("ERROR : %s\n", err)
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("ERROR : %s\n", err)
		return
	}

	fmt.Printf("Query OK. %d rows affected\n", rowsAffected)
}

func query(db *sql.DB, query string) {

	rows, err := db.Query(query)
	if err != nil {
		fmt.Printf("ERROR : %s\n", err)
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("ERROR : %s\n", err)
		return
	}

	// print rows name
	prettyPrintHeader(columns)

	for rows.Next() {
		holders := make([]interface{}, len(columns))
		err := rows.Scan(holders...)
		if err != nil {
			fmt.Printf("ERROR : %s\n", err)
			return
		}
		prettyPrintRow(holders)
	}
}

func prettyPrintHeader(row []string) {
	for i, r := range row {
		if i != 0 {
			fmt.Printf("|")
		}
		fmt.Printf("%10s", r)
	}
	fmt.Println()
}

func prettyPrintRow(row []interface{}) {
	for i, r := range row {
		if i != 0 {
			fmt.Printf("|")
		}
		fmt.Printf("%10s", r)
	}
	fmt.Println()
}

func loop(db *sql.DB) {
	// Readline
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("ramsql> ")
		buffer, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Printf("exit\n")
				return
			}

			fmt.Printf("Reading error\n")
			return
		}
		buffer = buffer[:len(buffer)-1]

		if len(buffer) == 0 {
			continue
		}

		// Do things here
		stmt := string(buffer)
		if strings.HasPrefix(stmt, "SELECT") {
			query(db, stmt)
		} else if strings.HasPrefix(stmt, "SHOW") {
			query(db, stmt)
		} else if strings.HasPrefix(stmt, "DESCRIBE") {
			query(db, stmt)
		} else {
			exec(db, stmt)
		}
	}
}

func main() {

	db, err := sql.Open("ramsql", "")
	if err != nil {
		fmt.Printf("Error : %s\n")
	}
	loop(db)
}
