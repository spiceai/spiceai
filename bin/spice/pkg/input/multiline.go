package input

import (
	"fmt"
	"io"
	"strings"

	"github.com/peterh/liner"
)

// ReadMultiLineInput reads input from the user until a complete statement is detected.
// A statement is considered complete when it:
// - Ends with a semicolon, OR
// - Is a special command (help, exit, quit) on the first line
//
// Returns the complete input string and any error encountered.
// If Ctrl+C is pressed on an empty prompt, returns io.EOF to signal exit.
// If Ctrl+C is pressed with partial input, returns empty string and nil error to cancel.
func ReadMultiLineInput(line *liner.State, prompt string) (string, error) {
	var query strings.Builder
	firstLine := true
	showPrompt := true

	for {
		var inputLine string
		var err error

		if showPrompt {
			inputLine, err = line.Prompt(prompt)
		} else {
			// For continuation lines, use empty prompt to avoid showing prompt again
			inputLine, err = line.Prompt("")
		}

		if err == liner.ErrPromptAborted {
			// Ctrl+C pressed
			if query.Len() == 0 && firstLine {
				// Ctrl+C on empty prompt - signal exit
				return "", io.EOF
			}
			// Ctrl+C with partial input - cancel and return empty
			return "", nil
		} else if err == io.EOF {
			// EOF reached (Ctrl+D or piped input exhausted)
			if query.Len() == 0 {
				return "", io.EOF
			}
			// If there's partial input, treat EOF as cancellation
			return "", nil
		} else if err != nil {
			return "", fmt.Errorf("reading line: %w", err)
		}

		// Add the line to the query buffer
		if query.Len() > 0 {
			query.WriteString("\n")
		}
		query.WriteString(inputLine)

		// Check if we should stop reading (semicolon at end or special command)
		trimmedQuery := strings.TrimSpace(query.String())
		lowerQuery := strings.ToLower(trimmedQuery)

		// Check for special commands on first line only
		if firstLine && (lowerQuery == "help" || lowerQuery == "exit" || lowerQuery == "quit") {
			break
		}

		// Check if query ends with semicolon
		if strings.HasSuffix(trimmedQuery, ";") {
			break
		}

		firstLine = false
		showPrompt = false
	}

	return strings.TrimSpace(query.String()), nil
}
