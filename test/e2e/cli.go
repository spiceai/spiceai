package e2e

import (
	"os"
	"os/exec"
)

type cli struct {
	workingDir string
	cliPath    string
}

func (c *cli) runCliCmd(args ...string) error {
	cmd := exec.Command(c.cliPath, args...)
	cmd.Dir = c.workingDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		return err
	}

	return nil
}
