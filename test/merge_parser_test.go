package test

import (
	"testing"

	"github.com/stoolap/stoolap/internal/parser"
)

func TestMergeParser(t *testing.T) {
	// Create a parser
	p := parser.NewParser(parser.NewLexer("MERGE INTO target USING source ON 1=1"))

	// Parse the program without validating to avoid panics
	p.ParseProgram()

	// Look at parse errors
	t.Logf("Parse errors: %v", p.Errors())
}
