package aiengine

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/util"
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
		parts := strings.SplitN(line, "->", 2)
		if len(parts) == 2 {
			infoPart := parts[1]
			if strings.Contains(infoPart, "completed with score of") {
				message := aurora.BrightCyan(infoPart)
				log.Printf("%s->%s\n", parts[0], message)
				return
			}
		}

		log.Println(line)
	}

	context := context.CurrentContext()
	aiServerPath := filepath.Join(context.AIEngineDir(), pythonServerFilename)
	aiServerCmd = execCommand(context.AIEnginePythonCmdPath(), aiServerPath)
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

		fileLogger, err := loggers.NewFileLogger("aiengine", context.SpiceRuntimeDir())
		if err != nil {
			zaplog.Sugar().Errorf("error creating file logger: %w", err)
			fileLogger = nil
		}

		go func() {
			for outScanner.Scan() {
				line := outScanner.Text()
				if strings.Contains(line, "->") {
					if outputFormatter != nil {
						outputFormatter(line)
					} else {
						log.Println(line)
					}
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
	return aiServerReady
}

func IsServerHealthy() error {
	return util.IsAIEngineServerHealthy(aiengineClient)
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

		err := IsServerHealthy()
		if err != nil {
			zaplog.Debug(err.Error())
			continue
		}

		break
	}

	return attemptCount
}

func getPodInitForTraining(pod *pods.Pod) *aiengine_pb.InitRequest {
	fields := make(map[string]float64)

	globalActions := pod.Actions()
	var laws []string

	var dsInitSpecs []*aiengine_pb.DataSource
	for _, ds := range pod.DataSources() {
		for fqField, fqFieldInitializer := range ds.Fields() {
			fieldName := strings.ReplaceAll(fqField, ".", "_")
			fields[fieldName] = fqFieldInitializer
		}

		dsActions := make(map[string]string)
		for dsAction := range ds.DataspaceSpec.Actions {
			fqAction, ok := globalActions[dsAction]
			if ok {
				dsActions[dsAction] = strings.ReplaceAll(fqAction, ".", "_")
			}
		}

		for _, law := range ds.Laws() {
			laws = append(laws, strings.ReplaceAll(law, ".", "_"))
		}

		dsInitSpec := aiengine_pb.DataSource{
			Actions: dsActions,
		}
		if ds.DataspaceSpec.Data != nil {
			dsInitSpec.Connector = &aiengine_pb.DataConnector{
				Name:   ds.DataspaceSpec.Data.Connector.Name,
				Params: ds.DataspaceSpec.Data.Connector.Params,
			}
		} else {
			dsInitSpec.Connector = &aiengine_pb.DataConnector{
				Name: "localstate",
			}
		}

		dsInitSpecs = append(dsInitSpecs, &dsInitSpec)
	}

	var rewardInit *string
	if pod.PodSpec.Training != nil {
		rewardInitTrimmed := strings.TrimSpace(pod.PodSpec.Training.RewardInit)
		if rewardInitTrimmed != "" {
			rewardInit = &rewardInitTrimmed
		}
	}

	globalFields := pod.FieldNames()

	rewards := pod.Rewards()
	globalActionRewards := make(map[string]string)
	for actionName := range globalActions {
		globalActionRewards[actionName] = rewards[actionName]
		if rewardInit != nil {
			reward := *rewardInit + "\n" + rewards[actionName]
			for _, fieldName := range globalFields {
				reward = strings.ReplaceAll(reward, fieldName, strings.ReplaceAll(fieldName, ".", "_"))
			}
			globalActionRewards[actionName] = reward
		}
	}

	epoch := pod.Epoch().Unix()

	podInit := aiengine_pb.InitRequest{
		Pod:         pod.Name,
		EpochTime:   epoch,
		Period:      int64(pod.Period().Seconds()),
		Interval:    int64(pod.Interval().Seconds()),
		Granularity: int64(pod.Granularity().Seconds()),
		Datasources: dsInitSpecs,
		Fields:      fields,
		Actions:     globalActionRewards,
		Laws:        laws,
	}

	return &podInit
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
}
