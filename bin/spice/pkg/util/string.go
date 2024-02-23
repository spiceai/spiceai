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
