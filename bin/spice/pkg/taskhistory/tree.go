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
	"sort"
)

// TreeNode represents a node in the trace tree.
type TreeNode struct {
	TaskHistory TaskHistory
	Children    []*TreeNode
}

// buildTraceTree constructs a hierarchical tree from a list of TaskHistory entries.
func buildTraceTree(tasks []TaskHistory) *TreeNode {
	// Create a lookup map for SpanID -> Node
	nodeMap := make(map[string]*TreeNode)

	// Populate the node map
	for _, task := range tasks {
		nodeMap[task.SpanID] = &TreeNode{TaskHistory: task}
	}

	// Identify the root and assign children
	var root *TreeNode
	for _, node := range nodeMap {
		if node.TaskHistory.ParentSpanID != nil {
			// Find the parent node and attach as a child
			if parent, exists := nodeMap[*node.TaskHistory.ParentSpanID]; exists {
				parent.Children = append(parent.Children, node)
			}
		} else {
			// Root node (no parent)
			root = node
		}
	}

	// Sort children by SpanID for a consistent order
	sortTree(root)
	return root
}

// sortTree sorts the tree nodes recursively by SpanID
func sortTree(node *TreeNode) {
	if node == nil {
		return
	}
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].TaskHistory.StartTime.asTime().Before(node.Children[j].TaskHistory.StartTime.asTime())
	})
	for _, child := range node.Children {
		sortTree(child)
	}
}
