//go:build windows
// +build windows

package cmd

import (
	"os"
	"os/exec"
	"syscall"
)

func launchCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP,
		ParentProcess: syscall.Handle(os.Getppid()), // Get terminal's PID
	}
}
