package loggers

import (
	"bufio"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spiceai/spiceai/pkg/context"
)

type TensorboardLogger struct {
	LogDir string

	cmdMutex sync.Mutex
	cmd      *exec.Cmd
	address  string
}

func (t *TensorboardLogger) Name() string {
	return "TensorBoard"
}

func (l *TensorboardLogger) Open() (string, error) {
	l.cmdMutex.Lock()
	defer l.cmdMutex.Unlock()

	if l.cmd != nil && l.cmd.ProcessState != nil && !l.cmd.ProcessState.Exited() {
		return l.address, nil
	}

	rtcontext := context.CurrentContext()
	tensorboardCmd := filepath.Join(rtcontext.AIEngineBinDir(), "tensorboard")
	cmd := exec.Command(tensorboardCmd, "--reuse_port", "True", "--logdir", l.LogDir)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}
	defer stderrPipe.Close()

	outScanner := bufio.NewScanner(stderrPipe)

	startedLineChan := make(chan string, 1)

	go func() {
		for outScanner.Scan() {
			line := outScanner.Text()
			if strings.HasPrefix(line, "TensorBoard ") && strings.HasSuffix(line, "(Press CTRL+C to quit)") {
				startedLineChan <- line
				return
			}
		}
		startedLineChan <- ""
	}()

	err = cmd.Start()
	if err != nil {
		return "", err
	}

	startedLine := <-startedLineChan

	if outScanner.Err() != nil {
		return "", outScanner.Err()
	}

	if startedLine == "" {
		return "", fmt.Errorf("Tensorboard failed to start")
	}

	parts := strings.Split(startedLine, " ")
	if len(parts) < 5 {
		return "", fmt.Errorf("Tensorboard failed to start")
	}

	fmt.Printf("%s %s started %s %s\n", parts[0], parts[1], parts[2], parts[3])

	l.address = parts[3]

	return l.address, err
}
