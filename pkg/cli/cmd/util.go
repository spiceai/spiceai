package cmd

import "os"

func IsDebug() bool {
	return os.Getenv("SPICE_DEBUG") == "1"
}
