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

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

type TensorBoard struct {
	cmd *exec.Cmd
	url *url.URL
}

var (
	cmdMutex             sync.Mutex
	tensorboardInstances map[string]*TensorBoard
)

type TensorboardLogger struct {
	RunId   string
	LogDir  string
}

func (t *TensorboardLogger) Name() string {
	return "TensorBoard"
}

func (l *TensorboardLogger) Open() (*url.URL, error) {
	// Open to the run parent, to allow comparison across runs
	runsDir, err := filepath.Abs(filepath.Dir(l.LogDir))
	if err != nil {
		return nil, fmt.Errorf("failed to get runs dir: %w", err)
	}

	cmdMutex.Lock()
	defer cmdMutex.Unlock()

	if len(tensorboardInstances) == 0 {
		tensorboardInstances = make(map[string]*TensorBoard)
	} else {
		tb, ok := tensorboardInstances[runsDir]
		if ok && tb != nil {
			conn, err := net.DialTimeout("tcp", tb.url.Host, time.Second)
			if err == nil {
				conn.Close()
				return tb.url, nil
			}

			delete(tensorboardInstances, runsDir)
		}
	}

	args := []string{"--logdir", runsDir}

	rtcontext := context.CurrentContext()
	var tensorboardCmd string
	if rtcontext.Name() == "docker" {
		tensorboardCmd = "tensorboard"
		args = append(args, "--bind_all")
	} else {
		tensorboardCmd = filepath.Join(rtcontext.AIEngineBinDir(), "tensorboard")
	}

	cmd := exec.Command(tensorboardCmd, args...)

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	defer stderrPipe.Close()

	outScanner := bufio.NewScanner(stderrPipe)

	startedLineChan := make(chan string, 1)

	outBuilder := strings.Builder{}

	go func() {
		for outScanner.Scan() {
			line := outScanner.Text()
			outBuilder.WriteString(line)
			if util.IsDebug() {
				fmt.Println(aurora.BrightYellow(line))
			}
			if strings.HasPrefix(line, "TensorBoard ") && strings.HasSuffix(line, "(Press CTRL+C to quit)") {
				startedLineChan <- line
				return
			}
		}
		startedLineChan <- ""
	}()

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	startedLine := <-startedLineChan

	if outScanner.Err() != nil {
		return nil, outScanner.Err()
	}

	if startedLine == "" {
		return nil, fmt.Errorf("Tensorboard failed to start: %s", outBuilder.String())
	}

	parts := strings.Split(startedLine, " ")
	if len(parts) < 5 {
		return nil, fmt.Errorf("Tensorboard failed to start: %s", outBuilder.String())
	}

	url, err := url.Parse(parts[3])
	if err != nil {
		return nil, fmt.Errorf("Tensorboard failed to start: %w", err)
	}

	if rtcontext.Name() == "docker" {
		url.Host = fmt.Sprintf("localhost:%s", url.Port())
	}

	fmt.Printf("Opening %s %s %s %s\n", parts[0], parts[1], parts[2], url.String())

	tries := 0

	for {
		tries++
		if tries > 10 {
			return nil, fmt.Errorf("Tensorboard failed to start: timed out")
		}
		timeout := time.Second
		conn, _ := net.DialTimeout("tcp", url.Host, timeout)
		if conn != nil {
			conn.Close()
			break
		}
	}

	tensorboardInstances[runsDir] = &TensorBoard{
		cmd: cmd,
		url: url,
	}

	return url, err
}
