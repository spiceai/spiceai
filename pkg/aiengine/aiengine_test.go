package aiengine

import (
	go_context "context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/testutils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

var snapshotter = testutils.NewSnapshotter("../../test/assets/snapshots/aiengine")

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

	t.Run("StartServer() -- Happy Path", testStartServerFunc())
	t.Run("StartServer() -- Python server takes a few tries to return healthy", testStartServerHealthyLaterFunc())
}

func TestInfer(t *testing.T) {
	t.Run("Infer() -- Server not ready", testInferServerNotReadyFunc())
	t.Run("Infer() -- Expected url is called", testInferServerFunc())
}

func TestPod(t *testing.T) {
	manifestsToTest := []string{"trader.yaml", "trader-infer.yaml", "event-tags.yaml"}

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
		t.Cleanup(func() {
			aiengineClient = nil
		})

		// Go is not deterministic with array ordering, so we account for that by sending a specified order
		// in the initialize request. However this makes tests unstable since the order will change on each run.
		// Fix the test by assigning a specific ordering
		testActionOrdering := map[string]map[string]int32{
			"trader": {
				"buy":  0,
				"sell": 1,
				"hold": 2,
			},
			"event-tags": {
				"action_one": 0,
				"action_two": 1,
			},
		}

		mockAIEngineClient := &MockAIEngineClient{
			InitHandler: func(c go_context.Context, ir *aiengine_pb.InitRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				if val, ok := testActionOrdering[pod.Name]; ok {
					assert.Equal(t, len(ir.ActionsOrder), len(val))
					for action := range ir.ActionsOrder {
						_, ok := val[action]
						assert.True(t, ok)
					}

					ir.ActionsOrder = val
				}

				snapshotter.SnapshotTJson(t, ir)

				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
		}

		aiengineClient = mockAIEngineClient

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
		expectedTrainRequest := &aiengine_pb.StartTrainingRequest{
			Pod:               pod.Name,
			Flight:            "1",
			NumberEpisodes:    10,
			TrainingGoal:      pod.PodSpec.Training.Goal,
			LearningAlgorithm: "dql",
		}

		t.Cleanup(func() {
			aiengineClient = nil
		})

		mockAIEngineClient := &MockAIEngineClient{
			StartTrainingHandler: func(c go_context.Context, actualTrainRequest *aiengine_pb.StartTrainingRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				expectedTrainRequest.EpochTime = actualTrainRequest.EpochTime

				assert.Equal(t, expectedTrainRequest, actualTrainRequest)

				return &aiengine_pb.Response{
					Result: response,
				}, nil
			},
		}

		aiengineClient = mockAIEngineClient

		err := StartTraining(pod, "", -1)
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
		t.Cleanup(func() {
			aiServerReady = false
			aiengineClient = nil
		})

		podName := "pod_foo"
		tagName := "tag_bar"

		mockAIEngineClient := &MockAIEngineClient{
			GetInferenceHandler: func(c go_context.Context, inferenceRequest *aiengine_pb.InferenceRequest, co ...grpc.CallOption) (*aiengine_pb.InferenceResult, error) {
				assert.Equal(t, inferenceRequest.Pod, podName)
				assert.Equal(t, inferenceRequest.Tag, tagName)

				return &aiengine_pb.InferenceResult{
					Response: &aiengine_pb.Response{
						Result: "ok",
					},
				}, nil
			},
			GetHealthHandler: func(c go_context.Context, healthRequest *aiengine_pb.HealthRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
		}

		SetAIEngineClient(mockAIEngineClient)

		resp, err := Infer("pod_foo", "tag_bar")
		if assert.NoError(t, err) {
			assert.Equal(t, "ok", resp.Response.Result)
		}
	}
}

func testPythonCmdDockerContextFunc() func(*testing.T) {
	return func(t *testing.T) {
		rtcontext, err := context.NewContext("docker")
		assert.NoError(t, err)

		actual := rtcontext.AIEnginePythonCmdPath()
		assert.Equal(t, "python3", actual)
	}
}

func testPythonCmdBareMetalContextFunc() func(*testing.T) {
	return func(t *testing.T) {
		homePath := os.Getenv("HOME")
		expectedPython := filepath.Join(homePath, ".spice/bin/ai/venv/bin/python3")

		rtcontext, err := context.NewContext("metal")
		assert.NoError(t, err)

		actual := rtcontext.AIEnginePythonCmdPath()
		assert.Equal(t, expectedPython, actual)
	}
}

func testStartServerFunc() func(*testing.T) {
	return func(t *testing.T) {
		execCommand = testutils.GetScenarioExecCommand("HAPPY_PATH")
		t.Cleanup(func() {
			getClient = NewAIEngineClient
			aiengineClient = nil
		})

		mockAIEngineClient := &MockAIEngineClient{
			GetHealthHandler: func(c go_context.Context, healthRequest *aiengine_pb.HealthRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
		}

		getClient = func(target string) (AIEngineClient, error) {
			return mockAIEngineClient, nil
		}

		assert.Nil(t, aiServerCmd)
		ready := make(chan bool)
		err := StartServer(ready, false)
		assert.NoError(t, err)
		<-ready
		assert.NotNil(t, aiServerCmd)
		actualPythonCmd := aiServerCmd.Args[3]
		assert.Equal(t, filepath.Join(os.Getenv("HOME"), ".spice/bin/ai/venv/bin/python3"), actualPythonCmd)
		actualArg := aiServerCmd.Args[4]
		assert.Equal(t, filepath.Join(os.Getenv("HOME"), ".spice/bin/ai/main.py"), actualArg)
	}
}

func testStartServerHealthyLaterFunc() func(*testing.T) {
	return func(t *testing.T) {
		execCommand = testutils.GetScenarioExecCommand("HAPPY_PATH")
		t.Cleanup(func() {
			getClient = NewAIEngineClient
			aiengineClient = nil
		})

		healthyRequests := 0
		mockAIEngineClient := &MockAIEngineClient{
			GetHealthHandler: func(c go_context.Context, healthRequest *aiengine_pb.HealthRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				healthyRequests += 1

				if healthyRequests >= 5 {
					return &aiengine_pb.Response{
						Result: "ok",
					}, nil
				} else {
					return nil, fmt.Errorf("server not ready yet")
				}
			},
		}

		getClient = func(target string) (AIEngineClient, error) {
			return mockAIEngineClient, nil
		}

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
		podInitSpec.EpochTime = testEpochTime

		snapshotter.SnapshotTJson(t, podInitSpec)
	}
}
