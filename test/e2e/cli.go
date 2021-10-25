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
	cmd, err := c.startCliCmd(args...)
	if err != nil {
		return err
	}

	err = cmd.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (c *cli) startCliCmd(args ...string) (*exec.Cmd, error) {
	cmd := exec.Command(c.cliPath, args...)
	cmd.Dir = c.workingDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

func (c *cli) runCliCmdOutput(args ...string) ([]byte, error) {
	cmd := exec.Command(c.cliPath, args...)
	return cmd.Output()
}
