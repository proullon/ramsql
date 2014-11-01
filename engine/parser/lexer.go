package parser

import (
	"fmt"
	"log"
	"unicode"
)

const (
	SpaceToken = iota
	SemicolonToken
	CreateToken
	TableToken
	SelectToken
	StringToken
	QuoteToken
	DoubleQuoteToken
	CommaToken
)

type Decl struct {
	Token  int
	Lexeme string
}

type lexer struct {
	tokens         []Decl
	instruction    []byte
	instructionLen int
	pos            int
}

func (l *lexer) lex(instruction []byte) ([]Decl, error) {
	log.Printf("lexer.lex : <%s>", instruction)
	l.instructionLen = len(instruction)
	l.tokens = nil
	l.instruction = instruction
	l.pos = 0
	securityPos := 0

	for l.pos < l.instructionLen {
		l.MatchSemicolonToken()
		l.MatchSpaceToken()
		l.MatchCreateToken()
		l.MatchTableToken()
		l.MatchStringToken()

		if l.pos == securityPos {
			log.Printf("Cannot lex <%s>, stuck at pos %d -> [%c]", l.instruction, l.pos, l.instruction[l.pos])
			return nil, fmt.Errorf("Cannot lex instruction. Syntax error near ", instruction[l.pos:])
		}
		securityPos = l.pos
	}

	return l.tokens, nil
}

func (l *lexer) MatchSpaceToken() bool {

	if l.instruction[l.pos] == ' ' {
		t := Decl{
			Token:  SpaceToken,
			Lexeme: " ",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}

func (l *lexer) MatchCreateToken() bool {

	if l.instruction[l.pos] == 'C' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'R' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'E' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'A' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'T' &&
		l.pos+5 < l.instructionLen && l.instruction[l.pos+5] == 'E' {

		t := Decl{
			Token:  CreateToken,
			Lexeme: "CREATE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 6
		return true
	}

	return false
}

func (l *lexer) MatchTableToken() bool {

	if l.instruction[l.pos] == 'T' &&
		l.pos+1 < l.instructionLen && l.instruction[l.pos+1] == 'A' &&
		l.pos+2 < l.instructionLen && l.instruction[l.pos+2] == 'B' &&
		l.pos+3 < l.instructionLen && l.instruction[l.pos+3] == 'L' &&
		l.pos+4 < l.instructionLen && l.instruction[l.pos+4] == 'E' {

		t := Decl{
			Token:  TableToken,
			Lexeme: "TABLE",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 5
		return true
	}

	return false
}

func (l *lexer) MatchStringToken() bool {

	i := l.pos
	for i < l.instructionLen && unicode.IsLetter(rune(l.instruction[i])) {
		i++
	}

	if i != l.pos {
		t := Decl{
			Token:  StringToken,
			Lexeme: string(l.instruction[l.pos:i]),
		}
		l.tokens = append(l.tokens, t)
		l.pos = i
		return true
	}

	return false
}

func (l *lexer) MatchSemicolonToken() bool {

	if l.instruction[l.pos] == ';' {
		t := Decl{
			Token:  SemicolonToken,
			Lexeme: ";",
		}
		l.tokens = append(l.tokens, t)
		l.pos += 1
		return true
	}

	return false
}
