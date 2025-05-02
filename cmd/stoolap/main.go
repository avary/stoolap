package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	// Import driver
	_ "github.com/stoolap/stoolap/pkg/driver"
)

func main() {
	// Parse command line flags
	dbPath := flag.String("db", "file://stoolap.db?snapshot_interval=30&keep_snapshots=2", "Database path (file://<path> or memory://)")
	flag.Parse()

	// Open the database
	db, err := sql.Open("stoolap", *dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Ping the database to make sure it's working
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to database: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connected to database: %s\n", *dbPath)

	// Check if we're getting input from a pipe
	stat, _ := os.Stdin.Stat()
	isPipe := (stat.Mode() & os.ModeCharDevice) == 0

	if isPipe {
		// Read input line by line and execute when we see a blank line
		scanner := bufio.NewScanner(os.Stdin)
		var currentStatement strings.Builder

		for scanner.Scan() {
			line := scanner.Text()

			// Skip shell-style comment lines (for backward compatibility)
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}

			// Skip SQL-style comment lines
			trimmedLine := strings.TrimSpace(line)
			if strings.HasPrefix(trimmedLine, "--") ||
				(strings.HasPrefix(trimmedLine, "/*") && strings.HasSuffix(trimmedLine, "*/")) {
				continue
			}

			// If blank line and we have a statement, execute it
			if strings.TrimSpace(line) == "" && currentStatement.Len() > 0 {
				q := strings.TrimSpace(currentStatement.String())
				currentStatement.Reset()

				if q != "" {
					// Split the input by semicolons to handle multiple statements
					statements := splitSQLStatements(q)
					for _, stmt := range statements {
						trimmedStmt := strings.TrimSpace(stmt)
						if trimmedStmt == "" {
							continue
						}

						start := time.Now()
						err := executeQuery(db, trimmedStmt)
						elapsed := time.Since(start)

						if err != nil {
							fmt.Fprintf(os.Stderr, "Error: %v\n", err)
						} else {
							fmt.Printf("Query executed in %v\n", elapsed)
						}
					}
				}
			} else {
				// Add the line to the current statement
				currentStatement.WriteString(line)
				currentStatement.WriteString(" ")
			}
		}

		if scanner.Err() != nil {
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", scanner.Err())
			os.Exit(1)
		}

		// Execute any remaining statement
		if currentStatement.Len() > 0 {
			q := strings.TrimSpace(currentStatement.String())
			if q != "" {
				// Split the input by semicolons to handle multiple statements
				statements := splitSQLStatements(q)
				for _, stmt := range statements {
					trimmedStmt := strings.TrimSpace(stmt)
					if trimmedStmt == "" {
						continue
					}

					start := time.Now()
					err := executeQuery(db, trimmedStmt)
					elapsed := time.Since(start)

					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
						// Don't exit immediately, try to process other statements
					} else {
						fmt.Printf("Query executed in %v\n", elapsed)
					}
				}
			}
		}

		return
	}

	// Interactive mode - use the improved CLI
	cli, err := NewCLI(db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing CLI: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "CLI error: %v\n", err)
		os.Exit(1)
	}
}

func executeQuery(db *sql.DB, query string) error {
	ctx := context.Background()

	// Look for a special separator that indicates parameter values
	parts := strings.Split(query, " -- PARAMS: ")

	// Trim trailing semicolons and whitespace from the SQL part
	if len(parts) > 0 {
		parts[0] = strings.TrimSuffix(strings.TrimSpace(parts[0]), ";")
	}

	// Initialize parameters array
	var params []interface{}

	// If we have a parameter part
	if len(parts) > 1 {
		baseQuery := parts[0] // Already trimmed above
		paramString := strings.TrimSpace(parts[1])

		// Parse parameter values (comma-separated)
		paramValues := strings.Split(paramString, ",")
		for _, val := range paramValues {
			// Trim whitespace and add to params
			params = append(params, convertParamValue(strings.TrimSpace(val)))
		}

		// Replace the query with just the SQL part
		query = baseQuery
	}

	// Check if it's a query that returns rows (SELECT, SHOW, etc.)
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") {
		var rows *sql.Rows
		var err error

		if len(params) > 0 {
			// Use parameters
			rows, err = db.QueryContext(ctx, query, params...)
		} else {
			// No parameters
			rows, err = db.QueryContext(ctx, query)
		}

		if err != nil {
			return err
		}
		defer rows.Close()

		// Get the column names
		columns, err := rows.Columns()
		if err != nil {
			return err
		}

		// Print the column names
		for i, column := range columns {
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Print(column)
		}
		fmt.Println()

		// Print a separator
		for i := range columns {
			if i > 0 {
				fmt.Print("-+-")
			}
			fmt.Print("----")
		}
		fmt.Println()

		// Create a slice of interfaces for the row values
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Iterate over the rows
		var count int
		for rows.Next() {
			// Scan the row into the values slice
			if err := rows.Scan(scanArgs...); err != nil {
				return err
			}

			// Print the values
			for i, value := range values {
				if i > 0 {
					fmt.Print(" | ")
				}
				printValue(value)
			}
			fmt.Println()
			count++
		}

		if err := rows.Err(); err != nil {
			return err
		}

		fmt.Printf("%d rows in set\n", count)
	} else {
		// Execute a non-query statement
		var result sql.Result
		var err error

		if len(params) > 0 {
			// Use parameters
			result, err = db.ExecContext(ctx, query, params...)
		} else {
			// No parameters
			result, err = db.ExecContext(ctx, query)
		}

		if err != nil {
			return err
		}

		// Print the result
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		fmt.Printf("%d rows affected\n", rowsAffected)
	}

	return nil
}

// convertParamValue tries to convert string parameter values to appropriate types
func convertParamValue(value string) interface{} {
	// Try to convert to integer
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}

	// Try to convert to float
	if f, err := strconv.ParseFloat(value, 64); err == nil && strings.Contains(value, ".") {
		return f
	}

	// Try to convert to boolean
	if b, err := strconv.ParseBool(value); err == nil && (value == "true" || value == "false") {
		return b
	}

	// Try to convert to time (ISO format)
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t
	}

	// If it's "null" or "NULL", return nil
	if strings.EqualFold(value, "null") {
		return nil
	}

	// Default to string
	return value
}

func printValue(value interface{}) {
	if value == nil {
		fmt.Print("NULL")
		return
	}

	switch v := value.(type) {
	case []byte:
		fmt.Print(string(v))
	case int64:
		fmt.Print(v)
	case float64:
		fmt.Print(v)
	case string:
		fmt.Print(v)
	case time.Time:
		fmt.Print(v.Format(time.RFC3339))
	case bool:
		fmt.Print(v)
	default:
		fmt.Print(v)
	}
}

// splitSQLStatements splits a SQL input into multiple statements based on semicolons
// This handles semicolons within quotes and avoids splitting those incorrectly
// It also handles SQL comments (both -- and /* */ style)
func splitSQLStatements(input string) []string {
	var statements []string
	var currentStatement strings.Builder

	// Keep track of parsing state
	inSingleQuotes := false
	inDoubleQuotes := false
	inLineComment := false
	inBlockComment := false

	// Process character by character
	for i := 0; i < len(input); i++ {
		char := input[i]

		// Handle end of line comment
		if inLineComment {
			if char == '\n' {
				inLineComment = false
				currentStatement.WriteByte(char) // Keep the newline
			}
			continue // Skip characters in line comments
		}

		// Handle start of line comment
		if !inSingleQuotes && !inDoubleQuotes && !inBlockComment &&
			char == '-' && i+1 < len(input) && input[i+1] == '-' {
			inLineComment = true
			i++ // Skip the second '-'
			continue
		}

		// Handle end of block comment
		if inBlockComment {
			if char == '*' && i+1 < len(input) && input[i+1] == '/' {
				inBlockComment = false
				i++ // Skip the '/'
			}
			continue // Skip characters in block comments
		}

		// Handle start of block comment
		if !inSingleQuotes && !inDoubleQuotes &&
			char == '/' && i+1 < len(input) && input[i+1] == '*' {
			inBlockComment = true
			i++ // Skip the '*'
			continue
		}

		// Handle quotes (if not in a comment)
		if !inBlockComment && !inLineComment {
			if char == '\'' && (i == 0 || input[i-1] != '\\') {
				inSingleQuotes = !inSingleQuotes
			} else if char == '"' && (i == 0 || input[i-1] != '\\') {
				inDoubleQuotes = !inDoubleQuotes
			}
		}

		// If we find a semicolon outside of quotes and comments, it's a statement delimiter
		if char == ';' && !inSingleQuotes && !inDoubleQuotes && !inBlockComment && !inLineComment {
			statements = append(statements, currentStatement.String())
			currentStatement.Reset()
		} else {
			currentStatement.WriteByte(char)
		}
	}

	// Add any remaining statement
	if currentStatement.Len() > 0 {
		statements = append(statements, currentStatement.String())
	}

	return statements
}
