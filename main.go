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
		fmt.Printf("ERROR : cannot execute : %s\n", err)
		return
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("ERROR : cannot get number of affected rows : %s\n", err)
		return
	}

	fmt.Printf("Query OK. %d rows affected\n", rowsAffected)
}

func query(db *sql.DB, query string) {

	rows, err := db.Query(query)
	if err != nil {
		fmt.Printf("ERROR : Cannot query : %s\n", err)
		return
	}

	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("ERROR : Cannot get columns name : %s\n", err)
		return
	}

	// print rows name
	prettyPrintHeader(columns)

	for rows.Next() {
		holders := make([]interface{}, len(columns))
		for i := range holders {
			holders[i] = new(string)
		}
		err := rows.Scan(holders...)
		if err != nil {
			fmt.Printf("ERROR : cannot scan values : %s\n", err)
			return
		}
		prettyPrintRow(holders)
	}
}

func prettyPrintHeader(row []string) {
	var line string

	fmt.Println()
	for i, r := range row {
		if i != 0 {
			line += fmt.Sprintf("  |  ")
		}
		line += fmt.Sprintf("%-6s", r)
	}
	fmt.Printf("%s\n", line)
	lineLen := len(line)
	for i := 0; i < lineLen; i++ {
		fmt.Printf("-")
	}
	fmt.Printf("\n")
}

func prettyPrintRow(row []interface{}) {
	for i, r := range row {
		if i != 0 {
			fmt.Printf("  |  ")
		}
		s, ok := r.(*string)
		if !ok {
			panic("wow sorry")
		}
		fmt.Printf("%-6s", *s)
	}
	fmt.Println()
}

func loop(db *sql.DB) {
	// Readline
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Printf("ramsql> ")
		buffer, err := reader.ReadBytes(';')
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

		stmt := string(buffer)
		stmt = strings.Replace(stmt, "\n", "", -1)

		// Do things here
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
		fmt.Printf("Error : cannot open connection : %s\n")
		return
	}
	loop(db)
}
