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

package display

import (
	"fmt"
	"strings"
)

// Table displays data in a formatted table with optional column types
func Table(colNames []string, colTypes []string, rows [][]string, colWidths []int) {
	// Print top border
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()

	// Print column names (centered)
	fmt.Print("|")
	for i, colName := range colNames {
		padding := colWidths[i] - len(colName)
		leftPad := padding / 2
		rightPad := padding - leftPad
		fmt.Printf(" %s%s%s |", strings.Repeat(" ", leftPad), colName, strings.Repeat(" ", rightPad))
	}
	fmt.Println()

	// Print column types if provided (centered)
	if colTypes != nil {
		fmt.Print("|")
		for i, colType := range colTypes {
			padding := colWidths[i] - len(colType)
			leftPad := padding / 2
			rightPad := padding - leftPad
			fmt.Printf(" %s%s%s |", strings.Repeat(" ", leftPad), colType, strings.Repeat(" ", rightPad))
		}
		fmt.Println()
	}

	// Print header separator
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()

	// Print all rows
	for _, row := range rows {
		// Split each cell by newlines to handle multi-line content
		maxLines := 1
		cellLines := make([][]string, len(row))
		for col, value := range row {
			cellLines[col] = strings.Split(value, "\n")
			if len(cellLines[col]) > maxLines {
				maxLines = len(cellLines[col])
			}
		}

		// Print each line of the row
		for lineIdx := 0; lineIdx < maxLines; lineIdx++ {
			fmt.Print("|")
			for col := range row {
				var lineValue string
				if lineIdx < len(cellLines[col]) {
					lineValue = cellLines[col][lineIdx]
				}
				fmt.Printf(" %-*s |", colWidths[col], lineValue)
			}
			fmt.Println()
		}
	}

	// Print bottom border
	fmt.Print("+")
	for _, width := range colWidths {
		fmt.Print(strings.Repeat("-", width+2))
		fmt.Print("+")
	}
	fmt.Println()
}
