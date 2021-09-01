package version

import "fmt"

// Values for these are injected by the build.
var (
	version = "local"
)

// Version returns the Spice version. This is either a semantic version
// number or else, in the case of unreleased code, the string "local".
func Version() string {
	if version == "local" {
		return version
	}

	return fmt.Sprintf("v%s", version)
}
