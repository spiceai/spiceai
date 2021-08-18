package aiengine

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/flights"
	"github.com/spiceai/spice/pkg/loggers"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/state"
	"github.com/spiceai/spice/pkg/util"
	"go.uber.org/zap"
)

type AIEngineResponse struct {
	Result  string `json:"result"`
	Message string `json:"message"`
}

const (
	pythonCmd   string = "python"
	aiServerUrl string = "http://localhost:8004"
)

var (
	execCommand                      = exec.Command
	HttpClient          *http.Client = http.DefaultClient
	retryClient         *http.Client
	pythonPath          string = filepath.Join(config.AiEnginePath(), "venv", "bin", pythonCmd)
	aiServerPath        string = filepath.Join(config.AiEnginePath(), "main.py")
	aiServerCmd         *exec.Cmd
	aiServerRunning     chan bool
	aiServerReady       bool        = false
	aiSingleTrainingRun bool        = false
	zaplog              *zap.Logger = loggers.ZapLogger()
)

func getPythonCmd() string {
	if context.CurrentContext() == context.Docker {
		return pythonCmd
	}

	return pythonPath
}

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

	aiServerCmd = execCommand(getPythonCmd(), aiServerPath)
	aiServerRunning := make(chan bool, 1)

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

		logFileName := loggers.FormatTimestampedLogFileName("ai-server")
		logPath := filepath.Join(config.SpiceLogPath(), logFileName)

		fileLogger := loggers.NewFileLogger(logPath)
		err := fileLogger.Open()
		if err != nil {
			log.Println(fmt.Errorf("error opening log file %s: %w", logPath, err))
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
					_ = fileLogger.Writeln(line)
				}
			}
		}()

		go func() {
			for errScanner.Scan() {
				line := errScanner.Text()

				if fileLogger != nil {
					_ = fileLogger.Writeln(line)
				}
			}
		}()

		err = aiServerCmd.Start()
		if err != nil {
			log.Println(fmt.Errorf("error starting %s: %w", aiServerCmd.Path, err))
			if fileLogger != nil {
				fileLogger.Close()
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
				fileLogger.Close()
			}

			if !isTestEnvironment() {
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
	return nil
}

func ServerReady() bool {
	return aiServerReady
}

func IsServerHealthy() error {
	return util.IsServerHealthy(aiServerUrl, HttpClient)
}

func InitializePod(pod *pods.Pod) error {

	err := pod.ValidateForTraining()
	if err != nil {
		return err
	}

	fields := make(map[string]float64)

	globalActions := pod.Actions()
	var laws []string

	var dsInitSpecs []spec.DataSourceInitSpec
	for _, ds := range pod.DataSources() {
		for fqField, fqFieldInitializer := range ds.Fields() {
			fieldName := strings.ReplaceAll(fqField, ".", "_")
			fields[fieldName] = fqFieldInitializer
		}

		dsActions := make(map[string]string)
		for dsAction := range ds.DataSourceSpec.Actions {
			fqAction, ok := globalActions[dsAction]
			if ok {
				dsActions[dsAction] = strings.ReplaceAll(fqAction, ".", "_")
			}
		}

		for _, law := range ds.Laws() {
			laws = append(laws, strings.ReplaceAll(law, ".", "_"))
		}

		dsInitSpec := spec.DataSourceInitSpec{
			Actions:   dsActions,
			Connector: *ds.DataSourceSpec.Connector,
		}
		dsInitSpecs = append(dsInitSpecs, dsInitSpec)
	}

	rewards := pod.Rewards()
	globalActionRewards := make(map[string]string)
	for actionName := range globalActions {
		globalActionRewards[actionName] = rewards[actionName]
	}

	epoch := pod.Epoch().Unix()

	podInit := spec.PodInitSpec{
		EpochTime:   &epoch,
		Period:      int64(pod.Period().Seconds()),
		Interval:    int(pod.Interval().Seconds()),
		Granularity: int(pod.Granularity().Seconds()),
		DataSources: dsInitSpecs,
		Fields:      fields,
		Actions:     globalActionRewards,
		Laws:        laws,
	}

	data, err := json.Marshal(podInit)
	if err != nil {
		return err
	}

	zaplog.Sugar().Debug(aurora.Yellow(string(data)))

	initUrl := fmt.Sprintf("%s/pods/%s/init", aiServerUrl, pod.Name)

	response, err := HttpClient.Post(initUrl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	if response.StatusCode != 200 {
		responseData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return fmt.Errorf("failed to verify training has started: %w", err)
		}

		var aiResponse AIEngineResponse
		err = json.Unmarshal(responseData, &aiResponse)
		if err != nil {
			return fmt.Errorf("failed to verify training has started: %w", err)
		}

		return fmt.Errorf("failed to validate manifest: %s", aiResponse.Result)
	}

	return nil
}

func StartTraining(pod *pods.Pod) error {
	flightId := fmt.Sprintf("%d", len(*pod.Flights())+1)

	flight := flights.NewFlight(flightId, int(pod.Episodes()))

	trainConfig := &spec.TrainSpec{
		EpochTime: pod.Epoch().Unix(),
		FlightId:  flightId,
		Episodes:  flight.ExpectedEpisodes(),
		Goal:      pod.PodSpec.Training.Goal,
	}

	data, err := json.Marshal(trainConfig)
	if err != nil {
		return err
	}

	zaplog.Sugar().Debug(aurora.Yellow(string(data)))

	trainUrl := fmt.Sprintf("%s/pods/%s/train", aiServerUrl, pod.Name)

	response, err := HttpClient.Post(trainUrl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("%s -> failed to start training: %w", pod.Name, err)
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("%s -> failed to verify training has started: %w", pod.Name, err)
	}

	var aiResponse AIEngineResponse
	err = json.Unmarshal(responseData, &aiResponse)
	if err != nil {
		return fmt.Errorf("%s -> failed to verify training has started: %w", pod.Name, err)
	}

	switch aiResponse.Result {
	case "already_training":
		return fmt.Errorf("%s -> training is already in progress", pod.Name)
	case "not_enough_data_for_training":
		return fmt.Errorf("%s -> insufficient data for training", pod.Name)
	case "epoch_time_invalid":
		return fmt.Errorf("%s -> epoch time %d invalid: %s", pod.Name, pod.Epoch().Unix(), aiResponse.Message)
	case "started_training":
		pod.AddFlight(flightId, flight)
		log.Println(fmt.Sprintf("%s -> %s", pod.Name, aurora.BrightCyan("Starting training...")))
	default:
		return fmt.Errorf("%s -> failed to verify training has started: %s", pod.Name, aiResponse.Result)
	}

	if !aiSingleTrainingRun {
		return nil
	}

	<-*flight.WaitForDoneChan()

	return nil
}

func SendData(pod *pods.Pod, podState ...*state.State) error {
	if len(podState) == 0 {
		// Nothing to do
		return nil
	}

	err := IsServerHealthy()
	if err != nil {
		return err
	}

	for _, s := range podState {
		if s == nil || !s.TimeSentToAIEngine.IsZero() {
			// Already sent
			continue
		}

		csv := strings.Builder{}
		csv.WriteString("time")
		for _, field := range s.Fields() {
			csv.WriteString(",")
			csv.WriteString(strings.ReplaceAll(field, ".", "_"))
		}
		csv.WriteString("\n")

		observationData := s.Observations()

		if len(observationData) == 0 {
			continue
		}

		csvChunk, csvPreview := observations.GetCsv(s.FieldNames(), observationData, 5)

		zaplog.Sugar().Debugf("Posting data to AI engine:\n%s", aurora.BrightYellow(fmt.Sprintf("%s%s...\n%d observations posted", csv.String(), csvPreview, len(observationData))))

		csv.WriteString(csvChunk)

		dataUrl := fmt.Sprintf("%s/pods/%s/data", aiServerUrl, pod.Name)

		_, err = HttpClient.Post(dataUrl, "text/plain; charset=utf-8", bytes.NewBufferString(csv.String()))
		if err != nil {
			return fmt.Errorf("failed to post new data to pod %s: %w", pod.Name, err)
		}

		s.Sent()
	}

	return err
}

func LoadInferencing(pod *pods.Pod, tag string) error {
	if tag == "" {
		tag = "latest"
	}

	url := fmt.Sprintf("%s/pods/%s/models/%s/load", aiServerUrl, pod.Name, tag)
	response, err := http.Post(url, "application/yaml", nil)
	if err != nil {
		return err
	}

	if response.StatusCode == 404 {
		return nil
	} else if response.StatusCode != 200 {
		log.Printf("Error: Failed to load model %d", response.StatusCode)
		return errors.New("unable to reload model after training")
	}

	return nil
}

func Infer(pod string, tag string) ([]byte, error) {
	if !ServerReady() {
		return nil, fmt.Errorf("not ready")
	}

	url := fmt.Sprintf("%s/pods/%s/models/%s/inference", aiServerUrl, pod, tag)

	if retryClient == nil {
		retryableClient := retryablehttp.NewClient()
		retryableClient.RetryMax = 3

		retryClient = retryableClient.StandardClient()
	}

	response, err := retryClient.Get(url)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
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

func getPodInitForTraining(pod *pods.Pod) *spec.PodInitSpec {
	fields := make(map[string]float64)

	globalActions := pod.Actions()
	var laws []string

	var dsInitSpecs []spec.DataSourceInitSpec
	for _, ds := range pod.DataSources() {
		for fqField, fqFieldInitializer := range ds.Fields() {
			fieldName := strings.ReplaceAll(fqField, ".", "_")
			fields[fieldName] = fqFieldInitializer
		}

		dsActions := make(map[string]string)
		for dsAction := range ds.DataSourceSpec.Actions {
			fqAction, ok := globalActions[dsAction]
			if ok {
				dsActions[dsAction] = strings.ReplaceAll(fqAction, ".", "_")
			}
		}

		for _, law := range ds.Laws() {
			laws = append(laws, strings.ReplaceAll(law, ".", "_"))
		}

		dsInitSpec := spec.DataSourceInitSpec{
			Actions: dsActions,
		}
		if ds.DataSourceSpec.Data != nil {
			dsInitSpec.Connector = ds.DataSourceSpec.Data.Connector
		} else {
			dsInitSpec.Connector = spec.DataConnectorSpec{
				Name: "localstate",
			}
		}

		dsInitSpecs = append(dsInitSpecs, dsInitSpec)
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

	podInit := spec.PodInitSpec{
		EpochTime:   &epoch,
		Period:      int64(pod.Period().Seconds()),
		Interval:    int(pod.Interval().Seconds()),
		Granularity: int(pod.Granularity().Seconds()),
		DataSources: dsInitSpecs,
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
