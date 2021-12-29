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
		d := spiceDebug == "1" || strings.EqualFold(spiceDebug, "true")
		isDebug = &d
	}

	return *isDebug
}
