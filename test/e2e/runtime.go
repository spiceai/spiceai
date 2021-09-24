package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
	"github.com/spiceai/spiceai/pkg/util"
)

type runtimeServer struct {
	baseUrl            string
	shouldStartRuntime bool
	runtimePath        string
	workingDirectory   string
	cli                *cli
	context            string
	cmd                *exec.Cmd
}

func (r *runtimeServer) startRuntime() error {
	if !r.shouldStartRuntime {
		return nil
	}

	var err error

	if r.context == "docker" {
		r.cmd, err = r.startDockerRuntime()
	} else {
		r.cmd, err = r.startMetalRuntime()
	}
	if err != nil {
		return err
	}

	err = r.waitForServerHealthy()
	if err != nil {
		return err
	}

	return nil
}

func (r *runtimeServer) shutdown() error {
	if r.cmd != nil {
		err := r.cmd.Process.Signal(os.Interrupt)
		if err != nil {
			return err
		}
		err = r.cmd.Wait()
		if err != nil {
			return err
		}
		r.cmd = nil
	}
	return nil
}

func (r *runtimeServer) startDockerRuntime() (*exec.Cmd, error) {
	runtimeCmd, err := r.cli.startCliCmd("run")
	if err != nil {
		return nil, err
	}

	return runtimeCmd, nil
}

func (r *runtimeServer) startMetalRuntime() (*exec.Cmd, error) {
	runtimeCmd := exec.Command(r.runtimePath)
	runtimeCmd.Dir = r.workingDirectory
	runtimeCmd.Stdout = os.Stdout
	runtimeCmd.Stderr = os.Stderr
	err := runtimeCmd.Start()
	if err != nil {
		return nil, err
	}

	return runtimeCmd, nil
}

func (r *runtimeServer) getRecommendation(podName string, tag string) (*aiengine_pb.InferenceResult, error) {
	var inference aiengine_pb.InferenceResult
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/models/%s/recommendation", r.baseUrl, podName, tag)
	err := r.internalGet(url, &inference)
	if err != nil {
		return nil, err
	}

	return &inference, nil
}

func (r *runtimeServer) postObservations(podName string, newObservations []byte) error {
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/observations", r.baseUrl, podName)
	resp, err := http.Post(url, "text/csv", bytes.NewReader(newObservations))
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("error posting new observations: %s", resp.Status)
	}

	return nil
}

func (r *runtimeServer) postDataspace(podName string, dataspaceFrom string, dataspaceName string, data []byte) error {
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/dataspaces/%s/%s", r.baseUrl, podName, dataspaceFrom, dataspaceName)
	resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("error posting new data to dataspace: %s", resp.Status)
	}

	return nil
}

func (r *runtimeServer) getPods() (string, error) {
	url := fmt.Sprintf("%s/api/v0.1/pods", r.baseUrl)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (r *runtimeServer) getObservations(podName string) (string, error) {
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/observations", r.baseUrl, podName)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func (r *runtimeServer) getFlights(podName string) ([]*runtime_pb.Flight, error) {
	var data []*runtime_pb.Flight
	err := r.internalGet(fmt.Sprintf("%s/api/v0.1/pods/%s/training_runs", r.baseUrl, podName), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *runtimeServer) postInterpretations(podName string, newInterpretations []byte) error {
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/interpretations", r.baseUrl, podName)
	resp, err := http.Post(url, "application/json", bytes.NewReader(newInterpretations))
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("error posting new interpretations: %s", resp.Status)
	}

	return nil
}

func (r *runtimeServer) getInterpretations(podName string, startTime int64, endTime int64) ([]*api.Interpretation, error) {
	var data []*api.Interpretation
	err := r.internalGet(fmt.Sprintf("%s/api/v0.1/pods/%s/interpretations", r.baseUrl, podName), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *runtimeServer) waitForTrainingToComplete(podName string, flight string, expectedEpisodes int) error {
	maxAttempts := 120
	attemptCount := 0
	for {
		time.Sleep(time.Second)

		if attemptCount++; attemptCount > maxAttempts {
			return fmt.Errorf("failed to verify training completed after %d attempts", attemptCount)
		}

		var flightResponse runtime_pb.Flight
		url := fmt.Sprintf("%s/api/v0.1/pods/%s/training_runs/%s", r.baseUrl, podName, flight)
		err := r.internalGet(url, &flightResponse)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		if len(flightResponse.Episodes) < expectedEpisodes {
			continue
		}

		break
	}

	return nil
}

func (r *runtimeServer) internalGet(url string, data interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status: %s\nbody:%s", resp.Status, body)
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	return nil
}

func (r *runtimeServer) waitForServerHealthy() error {
	maxAttempts := 20
	attemptCount := 0
	for {
		time.Sleep(time.Millisecond * 250)

		if attemptCount++; attemptCount > 4*maxAttempts {
			return fmt.Errorf("failed to verify health of %s after %d attempts", r.baseUrl, attemptCount)
		}

		err := util.IsRuntimeServerHealthy(r.baseUrl, http.DefaultClient)
		if err != nil {
			continue
		}

		break
	}

	return nil
}
