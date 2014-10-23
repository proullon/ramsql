package parser

func ParseInstruction(instruction string) ([]Instruction, error) {

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

	return instructions, nil
}
