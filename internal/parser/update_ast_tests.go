// +build ignore

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/ioutil"
	"strings"
)

// This script helps update AST test cases to use helper functions
func main() {
	// Read the test file
	src, err := ioutil.ReadFile("ast_test.go")
	if err != nil {
		panic(err)
	}

	content := string(src)

	// Define replacements
	replacements := []struct {
		old string
		new string
	}{
		// Integer literals
		{`&IntegerLiteral{Value: `, `makeIntegerLiteral(`},
		// Float literals  
		{`&FloatLiteral{Value: `, `makeFloatLiteral(`},
		// String literals
		{`&StringLiteral{Value: `, `makeStringLiteral(`},
		// Boolean literals
		{`&BooleanLiteral{Value: `, `makeBooleanLiteral(`},
		// Identifiers
		{`&Identifier{Value: `, `makeIdentifier(`},
		// Fix closing braces
		{`makeIntegerLiteral(42}`, `makeIntegerLiteral(42)`},
		{`makeFloatLiteral(3.14}`, `makeFloatLiteral(3.14)`},
	}

	// Apply replacements
	for _, r := range replacements {
		content = strings.ReplaceAll(content, r.old, r.new)
	}

	// Write back
	err = ioutil.WriteFile("ast_test.go", []byte(content), 0644)
	if err != nil {
		panic(err)
	}

	fmt.Println("Test file updated successfully!")
}