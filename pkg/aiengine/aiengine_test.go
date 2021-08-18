package aiengine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/aiengine"))

func TestAIEngineGetPythonCmd(t *testing.T) {
	origContext := context.CurrentContext()
	t.Cleanup(func() { context.SetContext(origContext) })

	t.Run("getPythonCmd() -- Docker Context", testPythonCmdDockerContextFunc())
	t.Run("getPythonCmd() -- BareMetal Context", testPythonCmdBareMetalContextFunc())
}

func TestAIEngineStartServer(t *testing.T) {
	origContext := context.CurrentContext()
	t.Cleanup(func() { context.SetContext(origContext) })
	t.Cleanup(func() { execCommand = exec.Command })
	origHttpClient := HttpClient
	t.Cleanup(func() { HttpClient = origHttpClient })

	t.Run("StartServer() -- Happy Path", testStartServerFunc())
	t.Run("StartServer() -- Python server takes a few tries to return healthy", testStartServerHealthyLaterFunc())
}

func TestInfer(t *testing.T) {
	t.Run("Infer() -- Server not ready", testInferServerNotReadyFunc())
	t.Run("Infer() -- Expected url is called", testInferServerFunc())
}

func TestPod(t *testing.T) {
	manifestsToTest := []string{"trader.yaml", "trader-infer.yaml", "cartpole-v1.yaml"}

	for _, manifestToTest := range manifestsToTest {
		manifestPath := filepath.Join("../../test/assets/pods/manifests", manifestToTest)

		pod, err := pods.LoadPodFromManifest(manifestPath)
		if err != nil {
			t.Error(err)
			return
		}

		t.Run(fmt.Sprintf("testGetPodInitForTrainingFunc() - %s", manifestToTest), testGetPodInitForTrainingFunc(pod))
		t.Run(fmt.Sprintf("InitializePod() - %s", manifestToTest), testInitializePod(pod))
		t.Run(fmt.Sprintf("StartTraining() already_training - %s", manifestToTest), testStartTrainingFunc(pod, "already_training"))
		t.Run(fmt.Sprintf("StartTraining() not_enough_data_for_training - %s", manifestToTest), testStartTrainingFunc(pod, "already_training"))
		t.Run(fmt.Sprintf("StartTraining() epoch_time_invalid - %s", manifestToTest), testStartTrainingFunc(pod, "epoch_time_invalid"))
		t.Run(fmt.Sprintf("StartTraining() started_training - %s", manifestToTest), testStartTrainingFunc(pod, "started_training"))
	}
}

func testInitializePod(pod *pods.Pod) func(t *testing.T) {
	return func(t *testing.T) {
		HttpClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case fmt.Sprintf("http://localhost:8004/pods/%s/init", pod.Name):
				reqBody, err := io.ReadAll(req.Body)
				if err != nil {
					t.Error(err)
					return nil, err
				}
				var actualInit spec.PodInitSpec
				err = json.Unmarshal(reqBody, &actualInit)
				if err != nil {
					t.Error(err)
					return nil, err
				}

				if pod.Name == "cartpole-v1" {
					// Epoch time is not specified for cartpole, so it will be "now"
					var testStaticEpochTime int64 = 123
					actualInit.EpochTime = &testStaticEpochTime
				}

				// marshal to JSON so the snapshot is easy to consume
				data, err := json.MarshalIndent(actualInit, "", "  ")
				if err != nil {
					t.Error(err)
				}

				snapshotter.SnapshotT(t, data)

				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("ok")),
					Header:     make(http.Header),
				}, nil
			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		err := InitializePod(pod)
		if pod.Name == "trader-infer" {
			switch err.Error() {
			case "action 'sell' references undefined 'args.price'\n":
				break
			case "action 'buy' references undefined 'args.price'\n":
				break
			default:
				assert.FailNow(t, "Unexpected error", err)
			}
		} else {
			assert.NoError(t, err)
		}
	}
}

func testStartTrainingFunc(pod *pods.Pod, response string) func(t *testing.T) {
	return func(t *testing.T) {
		expectedTrainConfig := spec.TrainSpec{
			FlightId: "1",
			Episodes: 10,
			Goal:     pod.PodSpec.Training.Goal,
		}

		HttpClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case fmt.Sprintf("http://localhost:8004/pods/%s/train", pod.Name):
				reqBody, err := io.ReadAll(req.Body)
				if err != nil {
					t.Error(err)
					return nil, err
				}
				var actualTrainConfig spec.TrainSpec
				err = json.Unmarshal(reqBody, &actualTrainConfig)
				if err != nil {
					t.Error(err)
					return nil, err
				}
				expectedTrainConfig.EpochTime = actualTrainConfig.EpochTime

				assert.Equal(t, expectedTrainConfig, actualTrainConfig)

				aiResponse := &AIEngineResponse{
					Result: response,
				}

				aiResponseBytes, err := json.Marshal(aiResponse)
				if err != nil {
					t.Error(err)
					return nil, err
				}

				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBuffer(aiResponseBytes)),
					Header:     make(http.Header),
				}, nil
			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		err := StartTraining(pod)
		switch response {
		case "already_training":
			assert.EqualError(t, err, fmt.Sprintf("%s -> training is already in progress", pod.Name))
		case "not_enough_data_for_training":
			assert.EqualError(t, err, fmt.Sprintf("%s -> insufficient data for training", pod.Name))
		case "epoch_time_invalid":
			assert.Error(t, err)
			errorString := err.Error()
			assert.Contains(t, errorString, "epoch time")
			assert.Contains(t, errorString, "invalid")
		case "started_training":
			assert.NoError(t, err)
		default:
			assert.NoError(t, err)
		}
	}
}

func testInferServerNotReadyFunc() func(*testing.T) {
	return func(t *testing.T) {
		aiServerReady = false
		_, err := Infer("pod_foo", "tag_bar")
		if assert.Error(t, err) {
			assert.Equal(t, "not ready", err.Error())
		}
	}
}

func testInferServerFunc() func(*testing.T) {
	return func(t *testing.T) {
		aiServerReady = true
		t.Cleanup(func() { aiServerReady = false })

		podName := "pod_foo"
		tagName := "tag_bar"
		expectedUrl := fmt.Sprintf("http://localhost:8004/pods/%s/models/%s/inference", podName, tagName)

		retryClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case expectedUrl:
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("ok")),
					Header:     make(http.Header),
				}, nil
			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		resp, err := Infer("pod_foo", "tag_bar")
		if assert.NoError(t, err) {
			assert.Equal(t, "ok", string(resp))
		}
	}
}

func testPythonCmdDockerContextFunc() func(*testing.T) {
	return func(t *testing.T) {
		context.SetContext(context.Docker)
		actual := getPythonCmd()
		assert.Equal(t, "python", actual)
	}
}

func testPythonCmdBareMetalContextFunc() func(*testing.T) {
	return func(t *testing.T) {
		homePath := os.Getenv("HOME")
		expectedPython := filepath.Join(homePath, ".spice/bin/ai/venv/bin/python")

		context.SetContext(context.BareMetal)
		actual := getPythonCmd()
		assert.Equal(t, expectedPython, actual)
	}
}

func testStartServerFunc() func(*testing.T) {
	return func(t *testing.T) {
		execCommand = testutils.GetScenarioExecCommand("HAPPY_PATH")
		HttpClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case "http://localhost:8004/health":
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("ok")),
					Header:     make(http.Header),
				}, nil
			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		assert.Nil(t, aiServerCmd)
		ready := make(chan bool)
		err := StartServer(ready, false)
		assert.NoError(t, err)
		<-ready
		assert.NotNil(t, aiServerCmd)
		actualPythonCmd := aiServerCmd.Args[3]
		assert.Equal(t, filepath.Join(os.Getenv("HOME"), ".spice/bin/ai/venv/bin/python"), actualPythonCmd)
		actualArg := aiServerCmd.Args[4]
		assert.Equal(t, filepath.Join(os.Getenv("HOME"), ".spice/bin/ai/main.py"), actualArg)
	}
}

func testStartServerHealthyLaterFunc() func(*testing.T) {
	return func(t *testing.T) {
		execCommand = testutils.GetScenarioExecCommand("HAPPY_PATH")

		healthyRequests := 0
		HttpClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case "http://localhost:8004/health":
				healthyRequests += 1

				if healthyRequests >= 5 {
					return &http.Response{
						StatusCode: 200,
						Body:       io.NopCloser(bytes.NewBufferString("ok")),
						Header:     make(http.Header),
					}, nil
				} else if healthyRequests >= 3 {
					return &http.Response{
						StatusCode: 500,
						Body:       io.NopCloser(bytes.NewBufferString("server not ready yet")),
						Header:     make(http.Header),
					}, nil
				} else {
					return nil, fmt.Errorf("server not ready yet")
				}
			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		ready := make(chan bool)
		err := StartServer(ready, false)
		assert.NoError(t, err)
		<-ready
	}
}

func testGetPodInitForTrainingFunc(pod *pods.Pod) func(*testing.T) {
	return func(t *testing.T) {
		podInitSpec := getPodInitForTraining(pod)

		// set static epoch time for snapshot testing
		var testEpochTime int64 = 1234
		podInitSpec.EpochTime = &testEpochTime

		// marshal to JSON so the snapshot is easy to consume
		data, err := json.MarshalIndent(podInitSpec, "", "  ")
		if err != nil {
			t.Error(err)
		}

		snapshotter.SnapshotT(t, data)
	}
}
