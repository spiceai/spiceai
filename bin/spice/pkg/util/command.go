/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
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
