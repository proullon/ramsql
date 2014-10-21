package engine

import (
	"fmt"
	"log"
	"strings"
)

type parsingFunc func(string) string

var (
	parsingFuncs = map[string]parsingFunc{
		"CREATE": parseCREATE,
	}
)

func parseCREATE(query string) (answer string) {
	return fmt.Sprintf("Not implemented")
}

func parse(query string) (answer string) {
	log.Printf("Engine.parse <%s>", query)

	tokens := strings.Split(query, " ")
	if parsingFuncs[tokens[0]] == nil {
		log.Printf("Engine.parse : Unknown keyword <%s>", tokens[0])
		return fmt.Sprintf("Unknown keyword <%s>", tokens[0])
	}

	return parsingFuncs[tokens[0]](query)
}
