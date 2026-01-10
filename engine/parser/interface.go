package parser

import (
	"errors"
	"strings"
)

// stripComments removes SQL comment lines (starting with --) from the instruction
func stripComments(instruction string) string {
	var lines []string
	for _, line := range strings.Split(instruction, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

// ParseInstruction calls lexer and parser, then return Decl tree for each instruction
func ParseInstruction(instruction string) ([]Instruction, error) {
	instruction = stripComments(instruction)

	l := lexer{}
	tokens, err := l.lex([]byte(instruction))
	if err != nil {
		return nil, err
	}

	p := parser{}
	instructions, err := p.parse(tokens)
	if err != nil {
		return nil, err
	}

	if len(instructions) == 0 {
		return nil, errors.New("Error in syntax near " + instruction)
	}

	return instructions, nil
}
