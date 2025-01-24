//go:build !windows
// +build !windows

package cmd

import (
	"os/exec"
)

func launchCmd(cmd *exec.Cmd) {
}
