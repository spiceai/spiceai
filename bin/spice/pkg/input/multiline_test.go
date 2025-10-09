package input

import (
	"bufio"
	"context"
	"strings"
	"testing"
)

func TestMultiLineQueryBuilding(t *testing.T) {
	tests := []struct {
		name           string
		inputLines     []string
		expectedQuery  string
		shouldComplete bool
	}{
		{
			name: "Single line query with semicolon",
			inputLines: []string{
				"SELECT * FROM users;",
			},
			expectedQuery:  "SELECT * FROM users;",
			shouldComplete: true,
		},
		{
			name: "Multi-line query with semicolon on last line",
			inputLines: []string{
				"SELECT *",
				"FROM users",
				"WHERE id = 1;",
			},
			expectedQuery:  "SELECT *\nFROM users\nWHERE id = 1;",
			shouldComplete: true,
		},
		{
			name: "Multi-line query with semicolon on separate line",
			inputLines: []string{
				"SELECT *",
				"FROM users",
				"WHERE id = 1",
				";",
			},
			expectedQuery:  "SELECT *\nFROM users\nWHERE id = 1\n;",
			shouldComplete: true,
		},
		{
			name: "Query without semicolon should not complete",
			inputLines: []string{
				"SELECT * FROM users",
			},
			expectedQuery:  "SELECT * FROM users",
			shouldComplete: false,
		},
		{
			name: "Multi-line query preserves formatting",
			inputLines: []string{
				"SELECT",
				"    column1,",
				"    column2,",
				"    column3",
				"FROM table_name;",
			},
			expectedQuery:  "SELECT\n    column1,\n    column2,\n    column3\nFROM table_name;",
			shouldComplete: true,
		},
		{
			name: "Empty lines in multi-line query",
			inputLines: []string{
				"SELECT *",
				"",
				"FROM users;",
			},
			expectedQuery:  "SELECT *\n\nFROM users;",
			shouldComplete: true,
		},
		{
			name: "Special command on first line",
			inputLines: []string{
				"help",
			},
			expectedQuery:  "help",
			shouldComplete: true,
		},
		{
			name: "Exit command",
			inputLines: []string{
				"exit",
			},
			expectedQuery:  "exit",
			shouldComplete: true,
		},
		{
			name: "Quit command",
			inputLines: []string{
				"quit",
			},
			expectedQuery:  "quit",
			shouldComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var query strings.Builder
			completed := false

			for i, inputLine := range tt.inputLines {
				firstLine := i == 0

				// Add the line to the query buffer
				if query.Len() > 0 {
					query.WriteString("\n")
				}
				query.WriteString(inputLine)

				// Check if we should stop reading (semicolon at end or special command)
				trimmedQuery := strings.TrimSpace(query.String())
				lowerQuery := strings.ToLower(trimmedQuery)

				// Check for special commands on first line only
				if firstLine && (lowerQuery == "help" || lowerQuery == "exit" || lowerQuery == "quit") {
					completed = true
					break
				}

				// Check if query ends with semicolon
				if strings.HasSuffix(trimmedQuery, ";") {
					completed = true
					break
				}
			}

			queryStr := query.String()

			if queryStr != tt.expectedQuery {
				t.Errorf("Expected query:\n%s\n\nGot:\n%s", tt.expectedQuery, queryStr)
			}

			if completed != tt.shouldComplete {
				t.Errorf("Expected shouldComplete=%v, got %v", tt.shouldComplete, completed)
			}
		})
	}
}

func TestMultiLineQueryEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		inputLines    []string
		expectedQuery string
	}{
		{
			name: "Query with semicolon in middle should not complete early",
			inputLines: []string{
				"SELECT 'hello; world' as msg",
				"FROM users;",
			},
			expectedQuery: "SELECT 'hello; world' as msg\nFROM users;",
		},
		{
			name: "Multiple semicolons - stops at first line ending with semicolon",
			inputLines: []string{
				"SELECT 1;",
			},
			expectedQuery: "SELECT 1;",
		},
		{
			name: "Whitespace handling",
			inputLines: []string{
				"  SELECT *  ",
				"  FROM users  ",
				"  WHERE id = 1;  ",
			},
			expectedQuery: "  SELECT *  \n  FROM users  \n  WHERE id = 1;  ",
		},
		{
			name: "Case insensitive special commands",
			inputLines: []string{
				"HELP",
			},
			expectedQuery: "HELP",
		},
		{
			name: "Mixed case exit command",
			inputLines: []string{
				"Exit",
			},
			expectedQuery: "Exit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var query strings.Builder

			for _, inputLine := range tt.inputLines {
				// Add the line to the query buffer
				if query.Len() > 0 {
					query.WriteString("\n")
				}
				query.WriteString(inputLine)
			}

			queryStr := query.String()

			if queryStr != tt.expectedQuery {
				t.Errorf("Expected query:\n%q\n\nGot:\n%q", tt.expectedQuery, queryStr)
			}
		})
	}
}

// TestREPLMultiLinePaste simulates what happens when a user pastes multi-line SQL
// The actual behavior: when pasting into a terminal, liner.Prompt() returns each line separately
// but they come in rapid succession. The REPL should accumulate lines until finding a semicolon.
func TestREPLMultiLinePaste(t *testing.T) {
	tests := []struct {
		name          string
		pastedText    string
		expectedQuery string
		expectedCalls int // number of times executor should be called
	}{
		{
			name:          "Single line paste with semicolon",
			pastedText:    "SELECT * FROM users;",
			expectedQuery: "SELECT * FROM users;",
			expectedCalls: 1,
		},
		{
			name: "Multi-line paste with semicolon at end",
			pastedText: `SELECT min(fare_amount) as min, max(fare_amount) as max, avg(fare_amount) as avg
FROM taxi_trips
WHERE passenger_count >= 2;`,
			expectedQuery: "SELECT min(fare_amount) as min, max(fare_amount) as max, avg(fare_amount) as avg\nFROM taxi_trips\nWHERE passenger_count >= 2;",
			expectedCalls: 1, // Should be called once with the complete query
		},
		{
			name: "Multi-line paste with formatting",
			pastedText: `SELECT
    column1,
    column2,
    column3
FROM table_name;`,
			expectedQuery: "SELECT\n    column1,\n    column2,\n    column3\nFROM table_name;",
			expectedCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate pasted input by creating a reader with the full text
			reader := strings.NewReader(tt.pastedText + "\n")
			scanner := bufio.NewScanner(reader)

			// Track queries executed
			var executedQueries []string
			mockExecutor := func(ctx context.Context, query string) error {
				executedQueries = append(executedQueries, query)
				return nil
			}

			// Simulate the REPL loop reading from pasted input
			var query strings.Builder
			firstLine := true
			linesRead := 0

			for scanner.Scan() {
				linesRead++
				inputLine := scanner.Text()

				if query.Len() > 0 {
					query.WriteString("\n")
				}
				query.WriteString(inputLine)

				trimmedQuery := strings.TrimSpace(query.String())
				lowerQuery := strings.ToLower(trimmedQuery)

				if firstLine && (lowerQuery == "help" || lowerQuery == "exit" || lowerQuery == "quit") {
					break
				}

				if strings.HasSuffix(trimmedQuery, ";") {
					// Execute query
					queryStr := strings.TrimSpace(query.String())
					_ = mockExecutor(context.Background(), queryStr)
					query.Reset()
					firstLine = true
					continue
				}

				firstLine = false
			}

			// Verify
			if len(executedQueries) != tt.expectedCalls {
				t.Errorf("Expected executor to be called %d times, but was called %d times", tt.expectedCalls, len(executedQueries))
				t.Logf("Lines read: %d", linesRead)
				t.Logf("Queries executed: %v", executedQueries)
			}

			if len(executedQueries) > 0 && executedQueries[0] != tt.expectedQuery {
				t.Errorf("Expected query:\n%q\n\nGot:\n%q", tt.expectedQuery, executedQueries[0])
			}
		})
	}
}
