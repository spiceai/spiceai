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
	"io"
	"strconv"
	"strings"
)

// PrintTreeFromTraces prints a hierarchical tree of TaskHistory entries to the provided writer.
// Expects all `traces` to be from the same trace (i.e. same `TraceId`).
// The `fn` function is used to format details to display on each task history line.
func PrintTreeFromTraces(w io.Writer, traces []TaskHistory, fn func(t *TaskHistory) string) {
	printTree(w, buildTraceTree(traces), "", true, fn)
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
func printTree(w io.Writer, node *TreeNode, indent string, isLast bool, fn func(t *TaskHistory) string) {
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

	fmt.Fprintf(w, "%s%s[%s] %s\n", indent, connector, node.TaskHistory.SpanID, fn(&node.TaskHistory))

	// Recurse for children
	newIndent := indent + "│ "
	if isLast {
		newIndent = indent + "  "
	}

	for i, child := range node.Children {
		printTree(w, child, newIndent, i == len(node.Children)-1, fn)
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
