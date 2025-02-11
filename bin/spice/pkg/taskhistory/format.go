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
)

// Represents a table row to display a `TaskHistory` with an additional column to display the
// tree structure of the trace.
type TaskHistoryRow struct {
	// The tree structure (i.e. with indentations, etc) for the `TaskHistory` row.
	Tree string
	Task TaskHistory
}

// Constructs an ordered list of `TaskHistoryRow` from a trace of `TaskHistory`s.
// Expects all `traces` to be from the same trace (i.e. same `TraceId`).
func TreeRowsFromTraces(traces []TaskHistory) []TaskHistoryRow {
	tree := buildTraceTree(traces)
	c := make(chan TaskHistoryRow)
	go func() {
		defer close(c)
		recurseThroughTree(c, tree, "", true)
	}()

	rows := make([]TaskHistoryRow, 0)
	for cc := range c {
		rows = append(rows, cc)
	}

	return rows
}

// Recurse through the tree and construct the formatted tree column. Push each row to the channel.
func recurseThroughTree(c chan TaskHistoryRow, node *TreeNode, indent string, isLast bool) {
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
		recurseThroughTree(c, child, newIndent, i == len(node.Children)-1)
	}
}
