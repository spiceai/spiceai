package version

import (
	"fmt"
	"strings"
)

var (
	// Values for these are injected by the build.
	version = "local"
)

// Version returns the Spice version. This is either a semantic version
// number or else, in the case of unreleased code, the string prefix "local".
func Version() string {
	if strings.HasPrefix(version, "local") {
		return version
	}

	return fmt.Sprintf("v%s", version)
}
