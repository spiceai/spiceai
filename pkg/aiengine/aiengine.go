package aiengine

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	spice_context "github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"go.uber.org/zap"
)

type AIEngineResponse struct {
	Result  string `json:"result"`
	Message string `json:"message"`
}

const (
	aiServerUrl          = "localhost:8004"
	pythonServerFilename = "main.py"
)

var (
	execCommand         = exec.Command
	aiengineClient      AIEngineClient
	getClient           = NewAIEngineClient
	aiServerCmd         *exec.Cmd
	aiServerRunning     chan bool
	aiServerReady       bool        = false
	aiSingleTrainingRun bool        = false
	zaplog              *zap.Logger = loggers.ZapLogger()
)

func StartServer(ready chan bool, isSingleRun bool) error {
	if aiServerRunning != nil {
		return errors.New("ai engine already started")
	}

	aiSingleTrainingRun = isSingleRun

	outputFormatter := func(line string) {
		if strings.Contains(line, "completed with score of") {
			log.Println(aurora.BrightCyan(line))
		} else {
			log.Println(line)
		}
	}

	rtcontext := spice_context.CurrentContext()
	aiServerPath := filepath.Join(rtcontext.AIEngineDir(), pythonServerFilename)
	aiServerCmd = execCommand(rtcontext.AIEnginePythonCmdPath(), aiServerPath)
	aiServerRunning := make(chan bool, 1)

	var err error
	aiengineClient, err = getClient(aiServerUrl)
	if err != nil {
		return err
	}

	go func() {
		if aiServerCmd == nil {
			aiServerRunning <- true
			return
		}

		stdOutPipe, pipeErr := aiServerCmd.StdoutPipe()
		if pipeErr != nil {
			log.Printf("Error creating stdout for App: %s\n", pipeErr.Error())
			aiServerRunning <- false
			return
		}

		stdErrPipe, pipeErr := aiServerCmd.StderrPipe()
		if pipeErr != nil {
			log.Printf("Error creating stderr for App: %s\n", pipeErr.Error())
			aiServerRunning <- false
			return
		}

		outScanner := bufio.NewScanner(stdOutPipe)
		errScanner := bufio.NewScanner(stdErrPipe)

		fileLogger, err := loggers.NewFileLogger("aiengine", rtcontext.SpiceRuntimeDir())
		if err != nil {
			zaplog.Sugar().Errorf("error creating file logger: %w", err)
			fileLogger = nil
		}

		go func() {
			for outScanner.Scan() {
				line := outScanner.Text()
				if outputFormatter != nil {
					outputFormatter(line)
				} else {
					log.Println(line)
				}

				if fileLogger != nil {
					fileLogger.Info(line)
				}
			}
		}()

		go func() {
			for errScanner.Scan() {
				line := errScanner.Text()

				if fileLogger != nil {
					fileLogger.Info(line)
				}
			}
		}()

		err = aiServerCmd.Start()
		if err != nil {
			log.Println(fmt.Errorf("error starting %s: %w", aiServerCmd.Path, err))
			if fileLogger != nil {
				_ = fileLogger.Sync()
			}
			aiServerRunning <- false
			return
		}

		go func() {
			waitForServerHealthy(30)
			aiServerReady = true
			ready <- true
			appErr := aiServerCmd.Wait()

			if appErr != nil {
				log.Println(fmt.Errorf("process %s exited with error: %w", aiServerCmd.Path, appErr))
			}
			if fileLogger != nil {
				_ = fileLogger.Sync()
			}

			if appErr != nil && !aiServerCmd.ProcessState.Success() && !isTestEnvironment() {
				// If the AI engine crashes, pass on its exit status
				os.Exit(aiServerCmd.ProcessState.ExitCode())
			}
		}()

		aiServerRunning <- true
	}()

	appRunStatus := <-aiServerRunning
	if !appRunStatus {
		zaplog.Sugar().Error("AI Engine failed to run")
	}

	return nil
}

func StopServer() error {
	aiServerReady = false
	if aiServerCmd != nil {
		err := aiServerCmd.Process.Kill()
		aiServerCmd = nil
		if err != nil {
			return err
		}
		if aiServerRunning != nil {
			<-aiServerRunning
		}
	}
	if aiengineClient != nil {
		err := aiengineClient.Close()
		if err != nil {
			return err
		}
		aiengineClient = nil
	}
	return nil
}

func ServerReady() bool {
	err := IsAIEngineHealthy()
	if err != nil {
		zaplog.Sugar().Debugf("aiengine not healthy: %s", err)
		return false
	}

	return true
}

func IsAIEngineHealthy() error {
	if !aiServerReady {
		return errors.New("aiengine not yet ready")
	}

	return isAIEngineServerHealthy()
}

func waitForServerHealthy(maxAttempts int) int {
	attemptCount := 0
	for {
		time.Sleep(time.Millisecond * 250)

		if aiServerCmd == nil {
			break
		}

		if attemptCount++; attemptCount > 4*maxAttempts {
			log.Fatalf("Error: Failed to verify health of %s after %d attempts\n", aiServerUrl, attemptCount)
			break
		}

		err := isAIEngineServerHealthy()
		if err != nil {
			zaplog.Debug(err.Error())
			continue
		}

		break
	}

	return attemptCount
}

func isAIEngineServerHealthy() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := aiengineClient.GetHealth(ctx, &aiengine_pb.HealthRequest{})
	if err != nil {
		return err
	}

	if resp.Error {
		return errors.New(resp.Result)
	}

	if resp.Result != "ok" {
		return errors.New(resp.Result)
	}

	return nil
}

func isTestEnvironment() bool {
	for _, envVar := range aiServerCmd.Env {
		if envVar == "GO_WANT_HELPER_PROCESS=1" {
			return true
		}
	}

	return false
}

func SetAIEngineClient(newClient AIEngineClient) {
	aiengineClient = newClient
	aiServerReady = newClient != nil
}
