/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

package taskhistory

import (
	"fmt"
	"strconv"
	"strings"
)

type TaskHistoryRow struct {
	/// The tree structure (i.e. with indentations, etc) for the `TaskHistory` row.
	Tree string
	Task TaskHistory
}

// Expects all `traces` to be from the same trace (i.e. same `TraceId`).
func TreeRowsFromTraces(traces []TaskHistory) []TaskHistoryRow {
	tree := buildTraceTree(traces)
	c := make(chan TaskHistoryRow)
	go func() {
		defer close(c)
		printTree(c, tree, "", true)
	}()

	rows := make([]TaskHistoryRow, 0)
	for cc := range c {
		rows = append(rows, cc)
	}

	return rows
}

func ConvertLabelsToString(labels map[string]string) string {
	var sb strings.Builder
	sb.WriteString("{")

	i := 0
	for key, value := range labels {
		if i > 0 {
			sb.WriteString(", ")
		}

		switch {
		case isBool(value):
			sb.WriteString(fmt.Sprintf("%s: %t", key, mustParseBool(value)))
		case isInt(value):
			sb.WriteString(fmt.Sprintf("%s: %d", key, mustParseInt(value)))
		default:
			sb.WriteString(fmt.Sprintf("%s: %s", key, value))
		}
		i++
	}

	sb.WriteString("}")
	return sb.String()
}

// printTree prints the tree in ASCII format.
func printTree(c chan TaskHistoryRow, node *TreeNode, indent string, isLast bool) {
	if node == nil {
		return
	}

	connector := "├── "
	if isLast {
		connector = "└── "
	}
	if indent == "" {
		connector = ""
	}
	c <- TaskHistoryRow{fmt.Sprintf("%s%s%s", indent, connector, node.TaskHistory.SpanID), node.TaskHistory}

	// Recurse for children
	newIndent := indent + "│ "
	if isLast {
		newIndent = indent + "  "
	}

	for i, child := range node.Children {
		printTree(c, child, newIndent, i == len(node.Children)-1)
	}
}

func isBool(s string) bool {
	_, err := strconv.ParseBool(s)
	return err == nil
}

func mustParseBool(s string) bool {
	b, _ := strconv.ParseBool(s)
	return b
}

func isInt(s string) bool {
	_, err := strconv.Atoi(s)
	return err == nil
}

func mustParseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
