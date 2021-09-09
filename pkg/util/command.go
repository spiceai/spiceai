package util

import (
	"fmt"
	"log"
	"os"
	"os/exec"
)

func RunCommand(cmd *exec.Cmd) error {
	if cmd == nil {
		return nil
	}

	sigCh := make(chan os.Signal, 1)

	go func() {
		err := cmd.Start()
		if err != nil {
			return
		}

		go func() {
			appErr := cmd.Wait()

			if appErr != nil {
				log.Println(fmt.Errorf("process %s exited with error: %w", cmd.Path, appErr))
			}
			sigCh <- os.Interrupt
		}()
	}()

	<-sigCh

	if cmd != nil && (cmd.ProcessState == nil || !cmd.ProcessState.Exited()) {
		err := cmd.Process.Kill()
		if err != nil {
			return err
		}
	}

	return nil
}
