package loggers_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/spiceai/spice/pkg/loggers"
)

func TestFormatTimestampedLogFileName(t *testing.T) {
	timeNow := time.Now().UTC().Format("20060102T150405Z")
	expectedName := fmt.Sprintf("%s-%s.log", "basename", timeNow)

	actualName := loggers.FormatTimestampedLogFileName("basename")

	if expectedName != actualName {
		t.Errorf("Expected: %s, got: %s", expectedName, actualName)
	}
}
