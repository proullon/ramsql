package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/proullon/ramsql/engine"
)

func loop(e *engine.Engine) {
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

	e, err := engine.New()
	if err != nil {
		fmt.Printf("Error : %s\n")
	}
	loop(e)
}
