package sql

import (
	"fmt"
	"strings"

	"github.com/stoolap/stoolap/internal/parser"
	"github.com/stoolap/stoolap/internal/storage"
)

// convertDataTypeFromString converts a string data type to storage.DataType
func convertDataTypeFromString(typeStr string) (storage.DataType, error) {
	switch strings.ToUpper(typeStr) {
	case "INT", "INTEGER":
		return storage.INTEGER, nil
	case "FLOAT", "REAL", "DOUBLE":
		return storage.FLOAT, nil
	case "TEXT", "VARCHAR", "CHAR", "STRING":
		return storage.TEXT, nil
	case "BOOLEAN", "BOOL":
		return storage.BOOLEAN, nil
	case "TIMESTAMP", "DATETIME":
		return storage.TIMESTAMP, nil
	case "DATE":
		return storage.DATE, nil
	case "TIME":
		return storage.TIME, nil
	case "JSON":
		return storage.JSON, nil
	default:
		return storage.TEXT, fmt.Errorf("unsupported data type: %s", typeStr)
	}
}

// ExtractColumnAliases extracts column aliases from column expressions
func ExtractColumnAliases(columns []parser.Expression) map[string]string {
	aliases := make(map[string]string)

	for i, col := range columns {
		// Handle explicit aliases (AS keyword)
		if alias, ok := col.(*parser.AliasedExpression); ok {
			// For aliased column identifiers, map the alias to the actual column name
			if ident, ok := alias.Expression.(*parser.Identifier); ok {
				// For simple column references, store the actual column name
				aliases[alias.Alias.Value] = ident.Value
			} else if cast, ok := alias.Expression.(*parser.CastExpression); ok {
				// For CAST expressions with aliases, map the alias to a description
				aliases[alias.Alias.Value] = fmt.Sprintf("CAST_%s", strings.ToLower(cast.TypeName))
			} else if infix, ok := alias.Expression.(*parser.InfixExpression); ok {
				// Handle arithmetic expressions with aliases
				if infix.Operator == "+" || infix.Operator == "-" || infix.Operator == "*" || infix.Operator == "/" || infix.Operator == "%" {
					// For arithmetic expressions, use a descriptive alias based on the operator
					opDescriptor := "arithmetic_expression"
					aliases[alias.Alias.Value] = opDescriptor

					// Also add the original column name to the aliases map
					columnKey := fmt.Sprintf("column%d", i+1)
					aliases[columnKey] = alias.Alias.Value
				} else {
					// For other expressions with operators, store the string representation
					exprString := infix.String()
					aliases[alias.Alias.Value] = exprString
				}
			} else {
				// For other expressions, store the string representation
				// This works for functions and complex expressions in the SELECT list
				exprString := alias.Expression.String()
				aliases[alias.Alias.Value] = exprString
			}
		} else if cast, ok := col.(*parser.CastExpression); ok {
			// For non-aliased CAST expressions, create an implicit alias
			implicitAlias := fmt.Sprintf("CAST_%s", strings.ToLower(cast.TypeName))
			aliases[implicitAlias] = cast.String()

			// Also add column1, column2, etc. aliases for backward compatibility
			columnKey := fmt.Sprintf("column%d", i+1)
			aliases[columnKey] = implicitAlias
		} else if function, ok := col.(*parser.FunctionCall); ok {
			// For non-aliased functions, create an implicit alias
			implicitAlias := fmt.Sprintf("%s_result", strings.ToLower(function.Function))
			aliases[implicitAlias] = function.String()

			// Also add column1, column2, etc. aliases for backward compatibility
			columnKey := fmt.Sprintf("column%d", i+1)
			aliases[columnKey] = implicitAlias
		} else if infix, ok := col.(*parser.InfixExpression); ok {
			// Handle non-aliased arithmetic expressions
			if infix.Operator == "+" || infix.Operator == "-" || infix.Operator == "*" || infix.Operator == "/" || infix.Operator == "%" {
				// Create an implicit alias based on the operator
				var opName string
				switch infix.Operator {
				case "+":
					opName = "addition"
				case "-":
					opName = "subtraction"
				case "*":
					opName = "multiplication"
				case "/":
					opName = "division"
				case "%":
					opName = "modulo"
				default:
					opName = "arithmetic"
				}

				implicitAlias := fmt.Sprintf("calculated_%s", opName)
				aliases[implicitAlias] = infix.String()

				// Also add column1, column2, etc. aliases for backward compatibility
				columnKey := fmt.Sprintf("column%d", i+1)
				aliases[columnKey] = implicitAlias

				// Add the calculated name directly
				aliases["calculated"] = implicitAlias
			}
		}
	}

	return aliases
}
