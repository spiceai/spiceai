package cmd

import (
	"io"
	"strings"
	"testing"
)

func TestDisplayJSONResults_VndSpiceaiFormat(t *testing.T) {
	jsonData := `{
		"data": [
			{"name": "Alice", "age": 30, "city": "New York"},
			{"name": "Bob", "age": 25, "city": "Los Angeles"},
			{"name": "Charlie", "age": 35, "city": "Chicago"}
		],
		"schema": {
			"fields": [
				{"name": "name", "type": {"name": "Utf8"}, "nullable": false},
				{"name": "age", "type": {"name": "Int32"}, "nullable": true},
				{"name": "city", "type": {"name": "Utf8"}, "nullable": true}
			]
		}
	}`

	rows, _, err := displayJSONResults([]byte(jsonData))
	if err != nil {
		t.Fatalf("displayJSONResults failed: %v", err)
	}

	if rows != 3 {
		t.Errorf("Expected 3 rows, got %d", rows)
	}
}

func TestDisplayJSONResults_VndSpiceaiFormatWithRows(t *testing.T) {
	jsonData := `{
		"rowCount": 3,
		"schema": [
			{"name": "name", "type": {"name": "VARCHAR"}},
			{"name": "age", "type": {"name": "INT"}},
			{"name": "city", "type": {"name": "VARCHAR"}}
		],
		"rows": [
			{"name": "Alice", "age": 30, "city": "New York"},
			{"name": "Bob", "age": 25, "city": "Los Angeles"},
			{"name": "Charlie", "age": 35, "city": "Chicago"}
		]
	}`

	rows, _, err := displayJSONResults([]byte(jsonData))
	if err != nil {
		t.Fatalf("displayJSONResults failed: %v", err)
	}

	if rows != 3 {
		t.Errorf("Expected 3 rows, got %d", rows)
	}
}

func TestDisplayJSONResults_PlainJSONFormat(t *testing.T) {
	jsonData := `[
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "Los Angeles"},
		{"name": "Charlie", "age": 35, "city": "Chicago"}
	]`

	rows, _, err := displayJSONResults([]byte(jsonData))
	if err != nil {
		t.Fatalf("displayJSONResults failed: %v", err)
	}

	if rows != 3 {
		t.Errorf("Expected 3 rows, got %d", rows)
	}
}

func TestDisplayJSONResults_EmptyResults(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
	}{
		{
			name: "Empty vnd format with data",
			jsonData: `{
				"data": [],
				"schema": {
					"fields": [
						{"name": "id", "type": {"name": "Int32"}, "nullable": false}
					]
				}
			}`,
		},
		{
			name: "Empty vnd format with rows",
			jsonData: `{
				"rowCount": 0,
				"schema": [
					{"name": "id", "type": {"name": "INT"}}
				],
				"rows": []
			}`,
		},
		{
			name:     "Empty plain JSON",
			jsonData: `[]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, _, err := displayJSONResults([]byte(tt.jsonData))
			if err != nil {
				t.Fatalf("displayJSONResults failed: %v", err)
			}

			if rows != 0 {
				t.Errorf("Expected 0 rows for empty results, got %d", rows)
			}
		})
	}
}

func TestDisplayJSONResults_InvalidJSON(t *testing.T) {
	invalidJSON := `{invalid json`

	_, _, err := displayJSONResults([]byte(invalidJSON))
	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestDisplayJSONResults_NullValues(t *testing.T) {
	jsonData := `[
		{"name": "Alice", "age": 30, "city": null},
		{"name": "Bob", "age": null, "city": "Los Angeles"},
		{"name": null, "age": 35, "city": "Chicago"}
	]`

	rows, _, err := displayJSONResults([]byte(jsonData))
	if err != nil {
		t.Fatalf("displayJSONResults failed: %v", err)
	}

	if rows != 3 {
		t.Errorf("Expected 3 rows with null values, got %d", rows)
	}
}

// TestParseCustomHeaders verifies the custom headers parsing logic
func TestParseCustomHeaders(t *testing.T) {
	tests := []struct {
		name        string
		headerFlags []string
		expected    map[string]string
		expectError bool
	}{
		{
			name:        "Single header",
			headerFlags: []string{"Authorization:Bearer token123"},
			expected:    map[string]string{"Authorization": "Bearer token123"},
			expectError: false,
		},
		{
			name:        "Multiple headers",
			headerFlags: []string{"X-Custom:value1", "Y-Custom:value2"},
			expected:    map[string]string{"X-Custom": "value1", "Y-Custom": "value2"},
			expectError: false,
		},
		{
			name:        "Header with multiple colons",
			headerFlags: []string{"X-URL:https://example.com:8080"},
			expected:    map[string]string{"X-URL": "https://example.com:8080"},
			expectError: false,
		},
		{
			name:        "Empty header list",
			headerFlags: []string{},
			expected:    map[string]string{},
			expectError: false,
		},
		{
			name:        "Invalid header format",
			headerFlags: []string{"InvalidHeader"},
			expected:    nil,
			expectError: true,
		},
		{
			name:        "Header with empty value",
			headerFlags: []string{"X-Empty:"},
			expected:    map[string]string{"X-Empty": ""},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary function to parse headers since it's inline in the command
			parseHeaders := func(headers []string) (map[string]string, error) {
				customHeaders := make(map[string]string)
				for _, header := range headers {
					parts := strings.SplitN(header, ":", 2)
					if len(parts) != 2 {
						return nil, io.ErrUnexpectedEOF // Using a standard error
					}
					customHeaders[parts[0]] = parts[1]
				}
				return customHeaders, nil
			}

			result, err := parseHeaders(tt.headerFlags)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d headers, got %d", len(tt.expected), len(result))
			}

			for key, expectedValue := range tt.expected {
				if actualValue, ok := result[key]; !ok {
					t.Errorf("Expected header %s not found", key)
				} else if actualValue != expectedValue {
					t.Errorf("For header %s: expected value %s, got %s", key, expectedValue, actualValue)
				}
			}
		})
	}
}
