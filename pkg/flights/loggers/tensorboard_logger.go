package loggers

import (
	"bufio"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/context"
)

var (
	cmdMutex             sync.Mutex
	tensorboardInstances map[string]*exec.Cmd
)

type TensorboardLogger struct {
	RunId   string
	LogDir  string
	address string
}

func (t *TensorboardLogger) Name() string {
	return "TensorBoard"
}

func (l *TensorboardLogger) Open() (string, error) {
	// Open to the run parent, to allow comparison across runs
	runsDir, err := filepath.Abs(filepath.Dir(l.LogDir))
	if err != nil {
		return "", fmt.Errorf("failed to get runs dir: %w", err)
	}

	cmdMutex.Lock()
	defer cmdMutex.Unlock()

	if len(tensorboardInstances) == 0 {
		tensorboardInstances = make(map[string]*exec.Cmd)
	}

	cmd, ok := tensorboardInstances[runsDir]
	if ok && cmd != nil {
		if cmd.ProcessState.Exited() {
			delete(tensorboardInstances, runsDir)
			cmd = nil
		} else {
			return l.address, nil
		}
	}

	rtcontext := context.CurrentContext()
	tensorboardCmd := filepath.Join(rtcontext.AIEngineBinDir(), "tensorboard")
	cmd = exec.Command(tensorboardCmd, "--logdir", runsDir)
	tensorboardInstances[runsDir] = cmd

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
		return "", fmt.Errorf("Tensorboard failed to start: %s", outScanner.Text())
	}

	parts := strings.Split(startedLine, " ")
	if len(parts) < 5 {
		return "", fmt.Errorf("Tensorboard failed to start: %s", outScanner.Text())
	}

	fmt.Printf("Opening %s %s %s %s\n", parts[0], parts[1], parts[2], parts[3])

	url, err := url.Parse(parts[3])
	if err != nil {
		return "", fmt.Errorf("Tensorboard failed to start: %w", err)
	}

	tries := 0

	for {
		tries++
		if tries > 10 {
			return "", fmt.Errorf("Tensorboard failed to start: timed out")
		}
		timeout := time.Second
		conn, _ := net.DialTimeout("tcp", url.Host, timeout)
		if conn != nil {
			conn.Close()
			break
		}
	}

	l.address = parts[3]

	return l.address, err
}
