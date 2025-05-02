package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/chzyer/readline"
)

// CLI represents an interactive command-line interface for working with the database
type CLI struct {
	db           *sql.DB
	historyFile  string
	readline     *readline.Instance
	maxTableSize int    // Max allowed table width
	timeFormat   string // Time format string for query timing
	ctx          context.Context
}

// NewCLI creates a new CLI instance
func NewCLI(db *sql.DB) (*CLI, error) {
	// Determine history file location (in user's home directory)
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "."
	}
	historyFile := homeDir + "/.stoolap_history"

	// Create readline instance with custom configuration
	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "\033[1;36m>\033[0m ",
		HistoryFile:     historyFile,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		HistorySearchFold: true, // Case-insensitive history search

		// Enable Vim-style key bindings
		VimMode: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize readline: %v", err)
	}

	return &CLI{
		db:           db,
		historyFile:  historyFile,
		readline:     rl,
		maxTableSize: 120,
		timeFormat:   "15:04:05",
		ctx:          context.Background(),
	}, nil
}

// Run starts the CLI
func (c *CLI) Run() error {
	fmt.Println("Stoolap SQL CLI")
	fmt.Println("Enter SQL commands, 'help' for assistance, or 'exit' to quit.")
	fmt.Println("Use Up/Down arrows for history, Ctrl+R to search history.")

	// Main loop
	for {
		line, err := c.readline.Readline()
		if err != nil {
			if err == io.EOF || err == readline.ErrInterrupt {
				break
			}
			return err
		}

		// Trim whitespace
		query := strings.TrimSpace(line)

		// Handle special commands
		if query == "" {
			continue
		}

		switch strings.ToLower(query) {
		case "exit", "quit", "\\q":
			return nil
		case "help", "\\h", "\\?":
			c.printHelp()
			continue
		}

		// Split and execute multiple statements if needed (separated by semicolons)
		statements := splitSQLStatements(query)
		for _, stmt := range statements {
			trimmedStmt := strings.TrimSpace(stmt)
			if trimmedStmt == "" {
				continue
			}

			// Execute the query
			start := time.Now()
			err := c.executeQuery(trimmedStmt)
			elapsed := time.Since(start)

			if err != nil {
				fmt.Fprintf(os.Stderr, "\033[1;31mError:\033[0m %v\n", err)
			} else {
				fmt.Printf("\033[1;32mQuery executed in %v\033[0m\n", elapsed)
			}
		}
	}

	return nil
}

// executeQuery executes a SQL query and displays the results
func (c *CLI) executeQuery(query string) error {
	// Check if it's a query that returns rows (SELECT, SHOW, etc.)
	upperQuery := strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(upperQuery, "SELECT") ||
		strings.HasPrefix(upperQuery, "SHOW") {
		return c.executeReadQuery(query)
	} else {
		return c.executeWriteQuery(query)
	}
}

// executeReadQuery executes a query that returns rows (SELECT, SHOW, etc.)
func (c *CLI) executeReadQuery(query string) error {
	// Execute the query
	rows, err := c.db.QueryContext(c.ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Get the column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Calculate column widths based on column names
	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	// Store the rows in memory to analyze and format them nicely
	var values [][]interface{}
	var strValues [][]string

	// Create a slice of interfaces for the row values
	scanArgs := make([]interface{}, len(columns))
	for i := range scanArgs {
		scanArgs[i] = &scanArgs[i]
	}

	// Iterate over the rows
	rowCount := 0
	for rows.Next() {
		// Create a slice of interfaces for the row values
		rowValues := make([]interface{}, len(columns))
		for i := range rowValues {
			rowValues[i] = new(interface{})
		}

		// Scan the row into the values slice
		if err := rows.Scan(rowValues...); err != nil {
			return err
		}

		// Extract actual values and convert to strings
		row := make([]interface{}, len(columns))
		strRow := make([]string, len(columns))
		for i, v := range rowValues {
			val := *(v.(*interface{}))
			row[i] = val
			strValue := formatValue(val, columnTypes[i])
			strRow[i] = strValue

			// Update column width if needed
			if len(strValue) > colWidths[i] {
				colWidths[i] = len(strValue)
			}
		}

		values = append(values, row)
		strValues = append(strValues, strRow)
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// Cap column widths to keep table from getting too wide
	totalWidth := 1 // Start with 1 for the left border
	for _, width := range colWidths {
		// Add 2 for padding and 1 for the separator
		totalWidth += width + 3
	}

	if totalWidth > c.maxTableSize {
		// Need to shrink columns
		excess := totalWidth - c.maxTableSize
		for i := range colWidths {
			// Shrink columns proportionally
			if colWidths[i] > 10 { // Don't shrink tiny columns
				reduction := (colWidths[i] * excess) / totalWidth
				if reduction > 0 {
					colWidths[i] -= reduction
					if colWidths[i] < 10 {
						colWidths[i] = 10 // Set a minimum
					}
				}
			}
		}
	}

	// Print the table header with style
	// Top border
	c.printTableBorder(colWidths, "top")

	// Column headers
	fmt.Print("│ ")
	for i, col := range columns {
		padding := colWidths[i]
		fmt.Printf("\033[1m%-*s\033[0m ", padding, truncateString(col, padding))
		if i < len(columns)-1 {
			fmt.Print("│ ")
		} else {
			fmt.Print("│")
		}
	}
	fmt.Println()

	// Header/data separator with double line
	c.printTableBorder(colWidths, "mid")

	// Print the data rows
	for _, row := range strValues {
		fmt.Print("│ ")
		for i, val := range row {
			fmt.Printf("%-*s ", colWidths[i], truncateString(val, colWidths[i]))
			if i < len(row)-1 {
				fmt.Print("│ ")
			} else {
				fmt.Print("│")
			}
		}
		fmt.Println()
	}

	// Bottom border
	c.printTableBorder(colWidths, "bottom")

	// Print summary
	var rowText string
	if rowCount == 1 {
		rowText = "row"
	} else {
		rowText = "rows"
	}
	fmt.Printf("\033[1;32m%d %s in set\033[0m\n", rowCount, rowText)

	return nil
}

// executeWriteQuery executes a query that doesn't return rows (INSERT, UPDATE, DELETE, etc.)
func (c *CLI) executeWriteQuery(query string) error {
	// Execute the query
	result, err := c.db.ExecContext(c.ctx, query)
	if err != nil {
		return err
	}

	// Get rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	// Print result with better formatting
	var rowText string
	if rowsAffected == 1 {
		rowText = "row"
	} else {
		rowText = "rows"
	}
	fmt.Printf("\033[1;32m%d %s affected\033[0m\n", rowsAffected, rowText)

	return nil
}

// printTableBorder prints a border line for the table
func (c *CLI) printTableBorder(colWidths []int, position string) {
	var left, mid, right, horiz string

	switch position {
	case "top":
		left, mid, right, horiz = "┌", "┬", "┐", "─"
	case "mid":
		left, mid, right, horiz = "├", "┼", "┤", "─"
	case "bottom":
		left, mid, right, horiz = "└", "┴", "┘", "─"
	default:
		left, mid, right, horiz = "├", "┼", "┤", "─"
	}

	fmt.Print(left)
	for i, width := range colWidths {
		for j := 0; j < width+2; j++ {
			fmt.Print(horiz)
		}
		if i < len(colWidths)-1 {
			fmt.Print(mid)
		}
	}
	fmt.Println(right)
}

// printHelp displays help information
func (c *CLI) printHelp() {
	fmt.Println("\033[1mStoolap SQL CLI Commands:\033[0m")
	fmt.Println("")
	fmt.Println("  \033[1;33mSQL Commands:\033[0m")
	fmt.Println("    SELECT ...             Execute a SELECT query")
	fmt.Println("    INSERT ...             Insert data into a table")
	fmt.Println("    UPDATE ...             Update data in a table")
	fmt.Println("    DELETE ...             Delete data from a table")
	fmt.Println("    CREATE TABLE ...       Create a new table")
	fmt.Println("    CREATE INDEX ...       Create an index on a column")
	fmt.Println("    SHOW TABLES            List all tables")
	fmt.Println("    SHOW CREATE TABLE ...  Show CREATE TABLE statement for a table")
	fmt.Println("    SHOW INDEXES FROM ...  Show indexes for a table")
	fmt.Println("")
	fmt.Println("  \033[1;33mSpecial Commands:\033[0m")
	fmt.Println("    exit, quit, \\q         Exit the CLI")
	fmt.Println("    help, \\h, \\?          Show this help message")
	fmt.Println("")
	fmt.Println("  \033[1;33mKeyboard Shortcuts:\033[0m")
	fmt.Println("    Up/Down arrow keys     Navigate command history")
	fmt.Println("    Ctrl+R                 Search command history")
	fmt.Println("    Ctrl+A                 Move cursor to beginning of line")
	fmt.Println("    Ctrl+E                 Move cursor to end of line")
	fmt.Println("    Ctrl+W                 Delete word before cursor")
	fmt.Println("    Ctrl+U                 Delete from cursor to beginning of line")
	fmt.Println("    Ctrl+K                 Delete from cursor to end of line")
	fmt.Println("    Ctrl+L                 Clear screen")
	fmt.Println("")
}

// truncateString truncates a string to the specified length and adds an ellipsis if needed
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// formatValue formats a value for display, based on its type
func formatValue(value interface{}, colType *sql.ColumnType) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		// Check if it's potentially a JSON value
		strVal := string(v)
		if (strings.HasPrefix(strVal, "{") && strings.HasSuffix(strVal, "}")) ||
			(strings.HasPrefix(strVal, "[") && strings.HasSuffix(strVal, "]")) {
			// Format JSON more nicely for viewing
			return strVal
		}
		return strVal
	case string:
		return v
	case int64, int32, int16, int8, int:
		return fmt.Sprintf("%d", v)
	case float64:
		// Format with appropriate precision
		if v == float64(int64(v)) {
			return fmt.Sprintf("%.1f", v) // Integer value, show one decimal
		}
		return fmt.Sprintf("%.4g", v) // General format with 4 significant digits
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		// Format time/date values nicely
		t := v
		if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
			// It's just a date
			return t.Format("2006-01-02")
		}
		if t.Year() == 0 || t.Year() == 1 {
			// It might be just a time
			return t.Format("15:04:05.999")
		}
		// It's a full timestamp
		return t.Format("2006-01-02 15:04:05.999")
	default:
		// Fall back to basic formatting
		return fmt.Sprintf("%v", v)
	}
}

// Close closes the CLI and cleans up resources
func (c *CLI) Close() error {
	if c.readline != nil {
		return c.readline.Close()
	}
	return nil
}
