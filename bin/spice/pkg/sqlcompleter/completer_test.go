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
	"testing"
)

func TestRefreshMetadata(t *testing.T) {
	c := New()

	// Mock metadata fetcher
	c.SetMetadataFetcher(func(ctx context.Context, query string) ([]string, error) {
		switch query {
		case "--autocomplete\nSELECT table_schema || '.' || table_name as full_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') UNION SELECT table_schema || '.' || table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY full_name":
			// Return schema-qualified names (simulating public.users, analytics.products, etc.)
			return []string{"public.users", "analytics.products", "public.orders"}, nil
		case "--autocomplete\nSELECT DISTINCT table_schema FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY table_schema":
			return []string{"public", "analytics"}, nil
		case "--autocomplete\nSELECT DISTINCT column_name FROM information_schema.columns ORDER BY column_name":
			return []string{"id", "name", "email", "price"}, nil
		case "--autocomplete\nSELECT name FROM list_udfs() ORDER BY name":
			return []string{"count", "sum", "avg"}, nil
		case "--autocomplete\nSELECT name FROM list_udtfs() ORDER BY name":
			return []string{"generate_series", "unnest"}, nil
		default:
			// For keywords query, return fallback
			return []string{}, nil
		}
	})

	// Refresh metadata
	err := c.RefreshMetadata(context.Background())
	if err != nil {
		t.Fatalf("RefreshMetadata failed: %v", err)
	}

	// Verify tables were populated - should have both qualified and unqualified names
	// public.users -> users, public.users
	// analytics.products -> analytics.products only
	// public.orders -> orders, public.orders
	// Total: 5 unique names
	if len(c.tables) != 5 {
		t.Errorf("Expected 5 table entries (with public schema unqualified), got %d: %v", len(c.tables), c.tables)
	}

	// Verify schemas were populated
	if len(c.schemas) != 2 {
		t.Errorf("Expected 2 schemas, got %d", len(c.schemas))
	}

	// Verify columns were populated
	if len(c.columns) != 4 {
		t.Errorf("Expected 4 columns, got %d", len(c.columns))
	}

	// Verify UDFs were populated
	if len(c.udfs) != 3 {
		t.Errorf("Expected 3 UDFs, got %d", len(c.udfs))
	}

	// Verify UDTFs were populated
	if len(c.udtfs) != 2 {
		t.Errorf("Expected 2 UDTFs, got %d", len(c.udtfs))
	}

	// Verify keywords fallback was used
	if len(c.keywords) == 0 {
		t.Error("Expected fallback keywords to be populated")
	}
}

func TestExtractCurrentWord(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedWord   string
		expectedBefore string
	}{
		{
			name:           "empty string",
			input:          "",
			expectedWord:   "",
			expectedBefore: "",
		},
		{
			name:           "single word",
			input:          "select",
			expectedWord:   "select",
			expectedBefore: "",
		},
		{
			name:           "partial keyword",
			input:          "sel",
			expectedWord:   "sel",
			expectedBefore: "",
		},
		{
			name:           "after space",
			input:          "select ",
			expectedWord:   "",
			expectedBefore: "select",
		},
		{
			name:           "multiple words",
			input:          "select * fr",
			expectedWord:   "fr",
			expectedBefore: "select *",
		},
		{
			name:           "with comma",
			input:          "select name,ag",
			expectedWord:   "ag",
			expectedBefore: "select name,",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			word, before := extractCurrentWord(tt.input)
			if word != tt.expectedWord {
				t.Errorf("extractCurrentWord(%q) word = %q, want %q", tt.input, word, tt.expectedWord)
			}
			if before != tt.expectedBefore {
				t.Errorf("extractCurrentWord(%q) before = %q, want %q", tt.input, before, tt.expectedBefore)
			}
		})
	}
}

func TestShouldSuggestTables(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "after from",
			input:    "select * from",
			expected: true,
		},
		{
			name:     "after join",
			input:    "select * from users join",
			expected: true,
		},
		{
			name:     "after where",
			input:    "select * from users where",
			expected: false,
		},
		{
			name:     "at start",
			input:    "",
			expected: false,
		},
		{
			name:     "after select",
			input:    "select",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldSuggestTables(tt.input)
			if result != tt.expected {
				t.Errorf("shouldSuggestTables(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestCompleterComplete(t *testing.T) {
	c := New()
	c.SetKeywords([]string{"select", "from", "where", "update", "insert"})
	c.SetTables([]string{"users", "products", "orders"})
	c.SetColumns([]string{"id", "name", "email", "price"})
	c.SetUDFs([]string{"count", "sum", "avg"})

	tests := []struct {
		name           string
		input          string
		expectedInList []string
		notInList      []string
	}{
		{
			name:           "keyword completion",
			input:          "sel",
			expectedInList: []string{"select "},
		},
		{
			name:           "table after from",
			input:          "select * from u",
			expectedInList: []string{"select * from users "},
			notInList:      []string{"select * from update "}, // Should not suggest keywords in this context
		},
		{
			name:           "column completion",
			input:          "select na",
			expectedInList: []string{"select name "},
		},
		{
			name:           "udf completion",
			input:          "select cou",
			expectedInList: []string{"select count"},
		},
		{
			name:           "multiple matches",
			input:          "select * from ",
			expectedInList: []string{"select * from users ", "select * from products ", "select * from orders "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := c.Complete(tt.input)

			// Check expected items are in results
			for _, expected := range tt.expectedInList {
				found := false
				for _, result := range results {
					if result == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Complete(%q) missing expected %q in results %v", tt.input, expected, results)
				}
			}
		})
	}
}

func TestPublicSchemaUnqualified(t *testing.T) {
	c := New()

	// Mock metadata fetcher with public and non-public schemas
	c.SetMetadataFetcher(func(ctx context.Context, query string) ([]string, error) {
		switch query {
		case "--autocomplete\nSELECT table_schema || '.' || table_name as full_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'runtime') UNION SELECT table_schema || '.' || table_name FROM information_schema.views WHERE table_schema NOT IN ('information_schema', 'runtime') ORDER BY full_name":
			// Return mix of public and non-public tables
			return []string{"public.tvmaze", "public.users", "analytics.metrics", "scp.task_history"}, nil
		default:
			return []string{}, nil
		}
	})

	// Refresh metadata
	err := c.RefreshMetadata(context.Background())
	if err != nil {
		t.Fatalf("RefreshMetadata failed: %v", err)
	}

	// Verify we have both qualified and unqualified for public schema
	hasQualified := false
	hasUnqualified := false
	hasNonPublic := false

	for _, table := range c.tables {
		if table == "public.tvmaze" {
			hasQualified = true
		}
		if table == "tvmaze" {
			hasUnqualified = true
		}
		if table == "analytics.metrics" {
			hasNonPublic = true
		}
		// Should NOT have unqualified non-public tables
		if table == "metrics" {
			t.Error("Should not have unqualified 'metrics' from analytics schema")
		}
	}

	if !hasQualified {
		t.Error("Should have qualified name 'public.tvmaze'")
	}
	if !hasUnqualified {
		t.Error("Should have unqualified name 'tvmaze' for public schema table")
	}
	if !hasNonPublic {
		t.Error("Should have qualified name 'analytics.metrics'")
	}

	// Test completion
	results := c.Complete("select * from tv")
	found := false
	for _, result := range results {
		// Complete() returns the full reconstructed line
		if strings.Contains(result, "tvmaze") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Should autocomplete 'tv' to include 'tvmaze', got: %v", results)
	}
}

func TestTabCompletionFromClause(t *testing.T) {
	c := New()

	// Set up completer with a public.tvmaze table
	c.SetMetadataFetcher(func(ctx context.Context, query string) ([]string, error) {
		switch {
		case strings.Contains(query, "table_schema || '.' || table_name"):
			return []string{"public.tvmaze", "public.users"}, nil
		default:
			return []string{}, nil
		}
	})

	err := c.RefreshMetadata(context.Background())
	if err != nil {
		t.Fatalf("RefreshMetadata failed: %v", err)
	}

	// Debug: print what tables we have
	t.Logf("Available tables: %v", c.tables)

	// Test the exact use case: "select * from tv" should complete to "select * from tvmaze "
	input := "select * from tv"
	results := c.Complete(input)

	t.Logf("Input: %q", input)
	t.Logf("Results: %v", results)

	// Check that we get a completion with tvmaze
	found := false
	var matchedResult string
	for _, result := range results {
		if strings.Contains(result, "tvmaze") && !strings.Contains(result, "public.tvmaze") {
			found = true
			matchedResult = result
			break
		}
	}

	if !found {
		t.Errorf("Tab completion failed for 'select * from tv'\nExpected: completion containing 'tvmaze' (not 'public.tvmaze')\nGot: %v", results)
	} else {
		t.Logf("✓ Found completion: %q", matchedResult)
	}
}

func TestSimpleTabCompletion(t *testing.T) {
	c := New()

	// Directly set tables - bypass metadata fetcher to test core logic
	c.SetTables([]string{"public.tvmaze", "tvmaze", "public.users", "users"})

	tests := []struct {
		name     string
		input    string
		wantWord string
	}{
		{
			name:     "from tv",
			input:    "select * from tv",
			wantWord: "tvmaze",
		},
		{
			name:     "FROM tv uppercase",
			input:    "SELECT * FROM tv",
			wantWord: "tvmaze",
		},
		{
			name:     "join tv",
			input:    "select * from users join tv",
			wantWord: "tvmaze",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := c.Complete(tt.input)
			t.Logf("Input: %q", tt.input)
			t.Logf("Results: %v", results)

			found := false
			for _, result := range results {
				if strings.Contains(result, tt.wantWord+" ") && !strings.Contains(result, "public."+tt.wantWord) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected completion containing %q (unqualified), got: %v", tt.wantWord, results)
			}
		})
	}
}

func TestCTECompletion(t *testing.T) {
	c := New()
	c.SetKeywords([]string{"with", "as", "select", "from"})
	c.SetTables([]string{"users", "orders"})

	tests := []struct {
		name     string
		input    string
		wantWord string
	}{
		{
			name:     "with keyword",
			input:    "wit",
			wantWord: "with",
		},
		{
			name:     "table after cte",
			input:    "with cte as (select * from u",
			wantWord: "users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := c.Complete(tt.input)
			found := false
			for _, result := range results {
				if strings.Contains(result, tt.wantWord+" ") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected completion containing %q, got: %v", tt.wantWord, results)
			}
		})
	}
}
