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

package util

import (
	"bufio"
	"strings"
)

func AddElementToString(contentToModify string, elementToAdd string, addAfter string, replaceLine bool) (string, bool) {

	if strings.Contains(contentToModify, elementToAdd) {
		return "", false
	}

	var lines []string

	scanner := bufio.NewScanner(strings.NewReader(contentToModify))

	scanner.Split(bufio.ScanLines)

	var checkNextLine bool = false
	var alreadyAdded bool = false
	for scanner.Scan() {
		thisLine := scanner.Text()

		for checkNextLine && strings.Contains(thisLine, "#") {
			lines = append(lines, thisLine)
			scanner.Scan()
			thisLine = scanner.Text()
		}

		if !alreadyAdded && checkNextLine {
			lines = append(lines, elementToAdd)
			checkNextLine = false
			alreadyAdded = true
			if replaceLine {
				scanner.Scan()
				thisLine = scanner.Text()
			}
		}

		if thisLine == addAfter {
			checkNextLine = true
		}

		lines = append(lines, thisLine)
	}

	// If the element to add belongs at the end of the file, add it now
	if checkNextLine && !alreadyAdded {
		lines = append(lines, elementToAdd)
	}

	output := strings.Join(lines, "\n")

	return output, true
}
