package util

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

func RunCommand(cmd *exec.Cmd) error {
	if cmd == nil {
		return nil
	}

	cmdErr := make(chan error, 1)
	cmdStopped := make(chan bool, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, os.Interrupt)

	err := cmd.Start()
	if err != nil {
		return err
	}

	go func() {
		appErr := cmd.Wait()

		if appErr != nil {
			cmdErr <- appErr
		}
		cmdStopped <- true
		sigCh <- os.Interrupt
	}()

	<-sigCh

	if cmd != nil && (cmd.ProcessState == nil || !cmd.ProcessState.Exited()) {
		err := cmd.Process.Signal(os.Interrupt)
		if err != nil {
			return err
		}
	}

	<-cmdStopped

	if len(cmdErr) > 0 {
		return <-cmdErr
	}

	return nil
}
