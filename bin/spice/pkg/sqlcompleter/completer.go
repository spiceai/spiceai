/*
Copyright 2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlcompleter

import (
	"context"
	"strings"
	"unicode"

	"github.com/peterh/liner"
)

// MetadataFetcher is a function that executes a query and returns string results
type MetadataFetcher func(ctx context.Context, query string) ([]string, error)

// Completer provides intelligent SQL autocomplete
type Completer struct {
	keywords    []string
	tables      []string
	schemas     []string
	columns     []string
	udfs        []string
	udtfs       []string
	historyFunc liner.Completer
	fetcher     MetadataFetcher
}

// New creates a new SQL completer
func New() *Completer {
	return &Completer{
		keywords: []string{},
		tables:   []string{},
		schemas:  []string{},
		columns:  []string{},
		udfs:     []string{},
		udtfs:    []string{},
	}
}

// SetMetadataFetcher sets the function to fetch metadata from the database
func (c *Completer) SetMetadataFetcher(fetcher MetadataFetcher) {
	c.fetcher = fetcher
}

// RefreshMetadata fetches fresh metadata from the database
func (c *Completer) RefreshMetadata(ctx context.Context) error {
	if c.fetcher == nil {
		return nil
	}

	// Fetch tables and views with schema information
	// We need both table_schema and table_name to handle public schema specially
	tablesWithSchema, err := c.fetcher(ctx, "--autocomplete\nSELECT table_schema || '.' || table_name as full_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') UNION SELECT table_schema || '.' || table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY full_name")
	if err == nil {
		// Process table names to handle public schema specially
		tableMap := make(map[string]bool)
		for _, fullName := range tablesWithSchema {
			// Always add the fully qualified name
			tableMap[fullName] = true

			// For public schema, also add unqualified name
			if len(fullName) > 7 && fullName[:7] == "public." {
				unqualifiedName := fullName[7:] // Remove "public." prefix
				tableMap[unqualifiedName] = true
			}
		}

		// Convert map to slice
		c.tables = make([]string, 0, len(tableMap))
		for name := range tableMap {
			c.tables = append(c.tables, name)
		}
	}

	// Fetch schemas
	schemas, err := c.fetcher(ctx, "--autocomplete\nSELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY table_schema")
	if err == nil {
		c.schemas = schemas
	}

	// Fetch columns
	columns, err := c.fetcher(ctx, "--autocomplete\nSELECT DISTINCT column_name FROM information_schema.columns ORDER BY column_name")
	if err == nil {
		c.columns = columns
	}

	// Fetch UDFs (User Defined Functions)
	udfs, err := c.fetcher(ctx, "--autocomplete\nSELECT name FROM list_udfs() ORDER BY name")
	if err == nil {
		c.udfs = udfs
	}

	// Fetch UDTFs (User Defined Table Functions)
	udtfs, err := c.fetcher(ctx, "--autocomplete\nSELECT name FROM list_udtfs() ORDER BY name")
	if err == nil {
		c.udtfs = udtfs
	}

	// Fetch built-in functions from information_schema.routines
	builtinFunctions, err := c.fetcher(ctx, "--autocomplete\nSELECT routine_name FROM information_schema.routines ORDER BY routine_name")
	if err == nil {
		c.udfs = append(c.udfs, builtinFunctions...)
	}

	// Fetch DataFusion keywords - use SQL standard keywords if query fails
	keywords, err := c.fetcher(ctx, "--autocomplete\nSELECT keyword FROM information_schema.df_settings WHERE name = 'datafusion.sql_parser.keywords'")
	if err == nil && len(keywords) > 0 {
		// Parse keywords if they come as a comma-separated string
		var allKeywords []string
		if len(keywords) == 1 {
			allKeywords = strings.Split(strings.ToLower(keywords[0]), ",")
			for i := range allKeywords {
				allKeywords[i] = strings.TrimSpace(allKeywords[i])
			}
		} else {
			allKeywords = keywords
		}

		// Filter to only include useful statement keywords and common clauses
		// Exclude reserved words that aren't useful as autocomplete suggestions
		c.keywords = filterUsefulKeywords(allKeywords)
	} else {
		// Fallback to common SQL keywords if we can't fetch from DataFusion
		c.keywords = []string{
			"select", "from", "where", "join", "inner", "left", "right", "outer",
			"on", "and", "or", "not", "in", "like", "between", "is", "null",
			"order", "by", "group", "having", "limit", "offset", "distinct",
			"as", "case", "when", "then", "else", "end", "union", "intersect",
			"except", "insert", "into", "values", "update", "set", "delete",
			"create", "table", "index", "view", "drop", "alter", "truncate",
			"desc", "asc", "primary", "key", "foreign", "references", "constraint",
			"unique", "check", "default", "with", "exists", "any", "all",
			"cross", "natural", "using", "cast", "extract", "interval",
			"show", "tables", "databases", "schemas", "columns",
		}
	}

	return nil
}

// filterUsefulKeywords filters the full list of DataFusion keywords to only include
// those that are useful as autocomplete suggestions (statement starters and common clauses)
func filterUsefulKeywords(allKeywords []string) []string {
	// Define useful keyword categories
	useful := map[string]bool{
		// Query statements
		"select": true, "from": true, "where": true, "having": true, "order": true, "group": true,
		"limit": true, "offset": true, "distinct": true, "union": true, "intersect": true, "except": true,

		// Join types
		"join": true, "inner": true, "left": true, "right": true, "full": true, "cross": true,
		"outer": true, "natural": true, "on": true, "using": true,

		// Boolean operators
		"and": true, "or": true, "not": true, "in": true, "exists": true, "between": true,
		"like": true, "ilike": true, "is": true, "null": true,

		// Common keywords
		"as": true, "asc": true, "desc": true, "by": true, "all": true, "any": true,
		"case": true, "when": true, "then": true, "else": true, "end": true,

		// DML statements
		"insert": true, "into": true, "values": true, "update": true, "set": true, "delete": true,

		// DDL statements
		"create": true, "alter": true, "drop": true, "truncate": true,
		"table": true, "view": true, "index": true, "schema": true, "database": true,

		// Constraints and keys
		"primary": true, "foreign": true, "key": true, "references": true,
		"unique": true, "constraint": true, "check": true, "default": true,

		// Functions and casts
		"cast": true, "extract": true, "interval": true, "current_date": true, "current_time": true,
		"current_timestamp": true,

		// CTEs and subqueries
		"with": true, "recursive": true,

		// SHOW commands
		"show": true, "tables": true, "schemas": true, "columns": true, "databases": true,

		// DESCRIBE/EXPLAIN
		"describe": true, "explain": true, "analyze": true,
	}

	var filtered []string
	for _, kw := range allKeywords {
		if useful[kw] {
			filtered = append(filtered, kw)
		}
	}

	// If filtering resulted in too few keywords, use the fallback list
	if len(filtered) < 20 {
		return []string{
			"select", "from", "where", "join", "inner", "left", "right", "outer",
			"on", "and", "or", "not", "in", "like", "between", "is", "null",
			"order", "by", "group", "having", "limit", "offset", "distinct",
			"as", "case", "when", "then", "else", "end", "union", "intersect",
			"except", "insert", "into", "values", "update", "set", "delete",
			"create", "table", "index", "view", "drop", "alter", "truncate",
			"desc", "asc", "primary", "key", "foreign", "references", "constraint",
			"unique", "check", "default", "with", "exists", "any", "all",
			"cross", "natural", "using", "cast", "extract", "interval",
			"show", "tables", "databases", "schemas", "columns",
		}
	}

	return filtered
}

// SetKeywords updates the list of SQL keywords
func (c *Completer) SetKeywords(keywords []string) {
	c.keywords = keywords
}

// SetTables updates the list of available tables
func (c *Completer) SetTables(tables []string) {
	c.tables = tables
}

// SetColumns updates the list of available columns
func (c *Completer) SetColumns(columns []string) {
	c.columns = columns
}

// SetUDFs updates the list of available user-defined functions
func (c *Completer) SetUDFs(udfs []string) {
	c.udfs = udfs
}

// SetHistoryCompleter sets the history-based completer
func (c *Completer) SetHistoryCompleter(historyFunc liner.Completer) {
	c.historyFunc = historyFunc
}

// Complete implements liner.Completer interface
func (c *Completer) Complete(line string) []string {
	if line == "" {
		return []string{}
	}

	// Extract the current word being typed
	word, beforeWord := extractCurrentWord(line)
	wordLower := strings.ToLower(word)
	beforeWordLower := strings.ToLower(beforeWord)

	var matches []string
	seen := make(map[string]bool)

	// Determine context based on what comes before
	shouldSuggestOnlyTables := shouldSuggestTables(beforeWordLower)

	if shouldSuggestOnlyTables {
		// Only suggest tables after FROM or JOIN
		for _, table := range c.tables {
			if strings.HasPrefix(strings.ToLower(table), wordLower) {
				completion := reconstructCompletion(line, word, table+" ")
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}
	} else {
		// Suggest keywords
		for _, keyword := range c.keywords {
			if strings.HasPrefix(keyword, wordLower) {
				completion := reconstructCompletion(line, word, keyword+" ")
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}

		// Suggest schemas
		for _, schema := range c.schemas {
			if strings.HasPrefix(strings.ToLower(schema), wordLower) {
				completion := reconstructCompletion(line, word, schema+".")
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}

		// Suggest UDFs
		for _, udf := range c.udfs {
			if strings.HasPrefix(strings.ToLower(udf), wordLower) {
				completion := reconstructCompletion(line, word, udf)
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}

		// Suggest UDTFs
		for _, udtf := range c.udtfs {
			if strings.HasPrefix(strings.ToLower(udtf), wordLower) {
				completion := reconstructCompletion(line, word, udtf)
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}

		// Suggest tables
		for _, table := range c.tables {
			if strings.HasPrefix(strings.ToLower(table), wordLower) {
				completion := reconstructCompletion(line, word, table+" ")
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}

		// Suggest columns
		for _, column := range c.columns {
			if strings.HasPrefix(strings.ToLower(column), wordLower) {
				completion := reconstructCompletion(line, word, column+" ")
				if !seen[completion] {
					matches = append(matches, completion)
					seen[completion] = true
				}
			}
		}
	}

	// Add history-based suggestions only if the line is completely empty
	if c.historyFunc != nil && line == "" {
		historyMatches := c.historyFunc(line)
		for _, match := range historyMatches {
			if !seen[match] {
				matches = append(matches, match)
				seen[match] = true
			}
		}
	}

	return matches
}

// extractCurrentWord extracts the word currently being typed and everything before it
func extractCurrentWord(line string) (word string, beforeWord string) {
	// Find the position of the last word boundary
	pos := len(line)
	for i := len(line) - 1; i >= 0; i-- {
		if isWordBoundary(rune(line[i])) {
			pos = i + 1
			break
		}
		if i == 0 {
			pos = 0
		}
	}

	word = line[pos:]
	beforeWord = strings.TrimRight(line[:pos], " \t\n\r")
	return word, beforeWord
}

// isWordBoundary returns true if the character is a word boundary
func isWordBoundary(ch rune) bool {
	return unicode.IsSpace(ch) ||
		ch == '(' || ch == ')' || ch == ',' || ch == ';' ||
		ch == '=' || ch == '<' || ch == '>' || ch == '!' ||
		ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' ||
		ch == '\'' || ch == '"' || ch == '`' ||
		ch == '.' || ch == '[' || ch == ']' || ch == '{' || ch == '}' ||
		ch == '|' || ch == '&' || ch == '^' || ch == '~'
}

// shouldSuggestTables returns true if we should only suggest tables (after FROM or JOIN)
func shouldSuggestTables(beforeWord string) bool {
	// Check if the last complete word is "from" or "join"
	words := strings.Fields(beforeWord)
	if len(words) == 0 {
		return false
	}

	lastWord := words[len(words)-1]
	return lastWord == "from" || lastWord == "join"
}

// reconstructCompletion rebuilds the full line with the completion
func reconstructCompletion(line, word, completion string) string {
	if word == "" {
		return line + completion
	}
	// Replace the current word with the completion
	pos := len(line) - len(word)
	return line[:pos] + completion
}
