package util

import (
	"os"
	"strings"
)

var (
	isDebug *bool
)

func IsDebug() bool {
	if isDebug == nil {
		spiceDebug := os.Getenv("SPICE_DEBUG")
		*isDebug = spiceDebug == "1" || strings.EqualFold(spiceDebug, "true")
	}

	return *isDebug
}
