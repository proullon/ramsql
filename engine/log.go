package engine

import (
	"log"
	"os"
)

func initLog() {
	log.SetOutput(os.Stdout)
}
