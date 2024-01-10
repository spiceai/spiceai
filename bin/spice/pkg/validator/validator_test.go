package validator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataspaceNameRegex(t *testing.T) {
	testCases := map[string]bool{
		"hi":                 true,
		"event_stream123":    true,
		"event-stream":       false,
		"    hello___ world": false,
		"d":                  true,
		"000asdf":            false,
		"asdf000":            true,
		"_asdf":              false,
		"a_asdf":             true,
	}

	for testCase, expectedMatch := range testCases {
		actualMatch := ValidateDataspaceName(testCase)
		assert.Equal(t, expectedMatch, actualMatch, fmt.Sprintf("unexpected dataspace name: %s", testCase))
	}
}
