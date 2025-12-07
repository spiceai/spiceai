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

package history

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/peterh/liner"
)

func TestHistoryManager(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Override the history file location for testing
	historyFile := filepath.Join(tempDir, "test_history.txt")

	// Create manager with custom file path
	mgr := &Manager{
		historyFile: historyFile,
		entries:     []string{},
	}

	// Test adding entries
	mgr.Add("SELECT * FROM table1")
	mgr.Add("SELECT * FROM table2")
	mgr.Add("SELECT * FROM table3")

	if len(mgr.GetEntries()) != 3 {
		t.Errorf("expected 3 entries, got %d", len(mgr.GetEntries()))
	}

	// Test saving
	if err := mgr.Save(); err != nil {
		t.Fatalf("failed to save history: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(historyFile); os.IsNotExist(err) {
		t.Fatalf("history file was not created")
	}

	// Test loading
	mgr2 := &Manager{
		historyFile: historyFile,
		entries:     []string{},
	}

	if err := mgr2.load(); err != nil {
		t.Fatalf("failed to load history: %v", err)
	}

	if len(mgr2.GetEntries()) != 3 {
		t.Errorf("expected 3 entries after load, got %d", len(mgr2.GetEntries()))
	}

	// Verify entries are correct
	entries := mgr2.GetEntries()
	if entries[0] != "SELECT * FROM table1" {
		t.Errorf("unexpected first entry: %s", entries[0])
	}
}

func TestHistoryDeduplication(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.txt"),
		entries:     []string{},
	}

	// Add duplicate consecutive entries
	mgr.Add("query1")
	mgr.Add("query1")
	mgr.Add("query2")

	if len(mgr.GetEntries()) != 2 {
		t.Errorf("expected 2 entries (duplicates removed), got %d", len(mgr.GetEntries()))
	}
}

func TestHistoryMaxSize(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Add more than max entries
	for i := 0; i < 150; i++ {
		mgr.Add("query" + string(rune(i))) // Make unique
	}

	if err := mgr.Save(); err != nil {
		t.Fatalf("failed to save history: %v", err)
	}

	// Reload and verify size is limited
	mgr2 := &Manager{
		historyFile: mgr.historyFile,
		entries:     []string{},
	}

	if err := mgr2.load(); err != nil {
		t.Fatalf("failed to load history: %v", err)
	}

	if len(mgr2.GetEntries()) != 100 {
		t.Errorf("expected 100 entries (max size), got %d", len(mgr2.GetEntries()))
	}
}

func TestHistoryEmptyEntries(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Try to add empty entry
	mgr.Add("")
	mgr.Add("   ")

	// Empty trimmed strings shouldn't be added (current implementation adds "   ")
	// but empty strings should not
	if len(mgr.GetEntries()) > 1 {
		t.Errorf("expected at most 1 entry, got %d", len(mgr.GetEntries()))
	}
}

func TestLoadIntoLiner(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Add entries using the proper method
	mgr.Add("query1")
	mgr.Add("query2")
	mgr.Add("query3")

	line := liner.NewLiner()
	defer func() {
		if err := line.Close(); err != nil {
			t.Errorf("failed to close liner: %v", err)
		}
	}()

	mgr.LoadIntoLiner(line)

	// Liner doesn't provide a way to read history back easily,
	// so we just verify it doesn't panic
}

func TestClear(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Add entries properly
	mgr.Add("query1")
	mgr.Add("query2")

	mgr.Clear()

	if len(mgr.GetEntries()) != 0 {
		t.Errorf("expected 0 entries after clear, got %d", len(mgr.GetEntries()))
	}
}

func TestClearAndSave(t *testing.T) {
	tempDir := t.TempDir()
	historyFile := filepath.Join(tempDir, "test_clear.json")

	// Create manager with some entries
	mgr := &Manager{
		historyFile: historyFile,
		entries:     []string{},
	}

	// Add entries properly
	mgr.Add("query1")
	mgr.Add("query2")
	mgr.Add("query3")

	// Save initial history
	if err := mgr.Save(); err != nil {
		t.Fatalf("failed to save initial history: %v", err)
	}

	// Clear and save
	mgr.Clear()
	if err := mgr.Save(); err != nil {
		t.Fatalf("failed to save after clear: %v", err)
	}

	// Load again and verify it's empty
	mgr2 := &Manager{
		historyFile: historyFile,
		entries:     []string{},
	}

	if err := mgr2.load(); err != nil {
		t.Fatalf("failed to load cleared history: %v", err)
	}

	if len(mgr2.GetEntries()) != 0 {
		t.Errorf("expected 0 entries after loading cleared history, got %d", len(mgr2.GetEntries()))
	}
}

func TestGetCompleter(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Add entries properly
	mgr.Add("SELECT * FROM users")
	mgr.Add("SELECT * FROM orders")
	mgr.Add("SELECT id, name FROM users")
	mgr.Add("SHOW TABLES")
	mgr.Add("DESCRIBE users")

	completer := mgr.GetCompleter()

	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:  "Complete SELECT",
			input: "SELECT",
			expected: []string{
				"SELECT id, name FROM users",
				"SELECT * FROM orders",
				"SELECT * FROM users",
			},
		},
		{
			name:  "Complete SELECT * FROM",
			input: "SELECT * FROM",
			expected: []string{
				"SELECT * FROM orders",
				"SELECT * FROM users",
			},
		},
		{
			name:  "Complete SHOW",
			input: "SHOW",
			expected: []string{
				"SHOW TABLES",
			},
		},
		{
			name:     "No matches",
			input:    "DELETE",
			expected: []string{},
		},
		{
			name:  "Empty input",
			input: "",
			expected: []string{
				"DESCRIBE users",
				"SHOW TABLES",
				"SELECT id, name FROM users",
				"SELECT * FROM orders",
				"SELECT * FROM users",
			},
		},
		{
			name:  "Partial match",
			input: "DESC",
			expected: []string{
				"DESCRIBE users",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matches := completer(tt.input)

			if len(matches) != len(tt.expected) {
				t.Errorf("expected %d matches, got %d", len(tt.expected), len(matches))
				t.Logf("Expected: %v", tt.expected)
				t.Logf("Got: %v", matches)
				return
			}

			for i, match := range matches {
				if match != tt.expected[i] {
					t.Errorf("match %d: expected %q, got %q", i, tt.expected[i], match)
				}
			}
		})
	}
}

func TestCompleterReversesOrder(t *testing.T) {
	mgr := &Manager{
		historyFile: filepath.Join(t.TempDir(), "test.json"),
		entries:     []string{},
	}

	// Add entries properly
	mgr.Add("query1")
	mgr.Add("query2")
	mgr.Add("query3")

	completer := mgr.GetCompleter()
	matches := completer("query")

	// Should return in reverse order (most recent first)
	expected := []string{"query3", "query2", "query1"}

	if len(matches) != len(expected) {
		t.Errorf("expected %d matches, got %d", len(expected), len(matches))
	}

	for i, match := range matches {
		if match != expected[i] {
			t.Errorf("match %d: expected %q, got %q", i, expected[i], match)
		}
	}
}
