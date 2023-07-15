package engine

/*
import (
	"fmt"

	"github.com/proullon/ramsql/engine/parser"
)

func ifExecutor(e *Engine, ifDecl *parser.Decl) error {

	if len(ifDecl.Decl) == 0 {
		return fmt.Errorf("malformed condition")
	}

	if e.opsExecutors[ifDecl.Decl[0].Token] != nil {
		return e.opsExecutors[ifDecl.Decl[0].Token](e, ifDecl.Decl[0], conn)
	}

	return fmt.Errorf("error near %v, unknown keyword", ifDecl.Decl[0].Lexeme)
}

func notExecutor(e *Engine, tableDecl *parser.Decl) error {
	return nil
}

func existsExecutor(e *Engine, tableDecl *parser.Decl) error {
	return nil
}
*/
