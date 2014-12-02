package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"

	_ "github.com/proullon/ramsql/driver"
)

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
	}
}

func main() {

	db, err := sql.Open("ramsql", "")
	if err != nil {
		fmt.Printf("Error : %s\n")
	}
	loop(db)
}
