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
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"
)

const (
	maxHistorySize = 100
)

// HistoryType represents the type of history being stored
type HistoryType string

const (
	QueryHistory  HistoryType = "query_history.txt"
	SearchHistory HistoryType = "search_history.txt"
	ChatHistory   HistoryType = "chat_history.txt"
)

// Manager handles persistent history storage for CLI commands
type Manager struct {
	historyFile string
	entries     []string
}

// NewManager creates a new history manager for the specified history type
func NewManager(historyType HistoryType) (*Manager, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("getting user home directory: %w", err)
	}

	spiceDir := filepath.Join(homeDir, ".spice")

	// Ensure .spice directory exists
	if err := os.MkdirAll(spiceDir, 0755); err != nil {
		return nil, fmt.Errorf("creating .spice directory: %w", err)
	}

	historyFile := filepath.Join(spiceDir, string(historyType))
	slog.Debug("History manager initialized", "file", historyFile)

	m := &Manager{
		historyFile: historyFile,
		entries:     []string{},
	}

	// Load existing history
	if err := m.load(); err != nil {
		// If file doesn't exist or can't be read, start with empty history
		// This is not an error condition
		slog.Debug("Starting with empty history", "error", err)
		m.entries = []string{}
	}

	return m, nil
}

// load reads the history from disk
func (m *Manager) load() error {
	data, err := os.ReadFile(m.historyFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No history file yet, not an error
		}
		return fmt.Errorf("reading history file: %w", err)
	}

	if len(data) == 0 {
		return nil // Empty file, not an error
	}

	// Parse plain text format (one entry per line)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			m.entries = append(m.entries, line)
		}
	}

	return scanner.Err()
}

// Save writes the current history to disk
func (m *Manager) Save() error {
	// Ensure we don't exceed max history size
	if len(m.entries) > maxHistorySize {
		m.entries = m.entries[len(m.entries)-maxHistorySize:]
	}

	// Write plain text format (one entry per line)
	data := strings.Join(m.entries, "\n")
	if len(m.entries) > 0 {
		data += "\n" // Add final newline
	}

	slog.Debug("Saving history", "file", m.historyFile, "entries", len(m.entries))
	if err := os.WriteFile(m.historyFile, []byte(data), 0600); err != nil {
		slog.Error("Failed to save history", "file", m.historyFile, "error", err)
		return fmt.Errorf("writing history file: %w", err)
	}
	slog.Debug("History saved successfully", "file", m.historyFile)

	return nil
}

// Add adds a new entry to the history
func (m *Manager) Add(entry string) {
	// Don't add empty entries
	if entry == "" {
		return
	}

	// Don't add duplicate consecutive entries
	if len(m.entries) > 0 && m.entries[len(m.entries)-1] == entry {
		return
	}

	slog.Debug("Adding entry to history", "entry", entry)
	m.entries = append(m.entries, entry)
}

// LoadIntoLiner loads the history into a liner instance for REPL use
func (m *Manager) LoadIntoLiner(line *liner.State) {
	for _, entry := range m.entries {
		line.AppendHistory(entry)
	}
}

// GetEntries returns all history entries
func (m *Manager) GetEntries() []string {
	return m.entries
}

// Clear removes all history entries
func (m *Manager) Clear() {
	m.entries = []string{}
}

// GetCompleter returns a completer function for liner that provides
// tab-completion based on history entries
func (m *Manager) GetCompleter() liner.Completer {
	return func(line string) []string {
		var matches []string

		// Find all history entries that start with the current line
		for _, entry := range m.entries {
			if len(entry) >= len(line) && entry[:len(line)] == line {
				matches = append(matches, entry)
			}
		}

		// Return matches in reverse order (most recent first)
		for i, j := 0, len(matches)-1; i < j; i, j = i+1, j-1 {
			matches[i], matches[j] = matches[j], matches[i]
		}

		return matches
	}
}

// GetHistoryEntries returns the history entries for use by other completers
func (m *Manager) GetHistoryEntries() []string {
	return m.entries
}
