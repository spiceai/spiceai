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

	"github.com/spiceai/spice/pkg/proto/aiengine_pb"
	"github.com/spiceai/spice/pkg/proto/runtime_pb"
	"github.com/spiceai/spice/pkg/util"
)

type runtimeServer struct {
	baseUrl string
}

func (r *runtimeServer) startRuntime(runtimePath string, workingDirectory string) (*exec.Cmd, error) {
	runtimeCmd := exec.Command(runtimePath)
	runtimeCmd.Dir = workingDirectory
	runtimeCmd.Stdout = os.Stdout
	runtimeCmd.Stderr = os.Stderr
	err := runtimeCmd.Start()
	if err != nil {
		return nil, err
	}

	err = r.waitForServerHealthy()
	if err != nil {
		return nil, err
	}

	return runtimeCmd, nil
}

func (r *runtimeServer) getInference(podName string, tag string) (*aiengine_pb.InferenceResult, error) {
	var inference aiengine_pb.InferenceResult
	url := fmt.Sprintf("%s/api/v0.1/pods/%s/models/%s/inference", r.baseUrl, podName, tag)
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
	err := r.internalGet(fmt.Sprintf("%s/api/v0.1/pods/%s/flights", r.baseUrl, podName), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (r *runtimeServer) waitForTrainingToComplete(podName string, flight string, expectedEpisodes int) error {
	maxAttempts := 20
	attemptCount := 0
	for {
		time.Sleep(time.Millisecond * 250)

		if attemptCount++; attemptCount > 4*maxAttempts {
			return fmt.Errorf("failed to verify training completed after %d attempts", attemptCount)
		}

		var flightResponse runtime_pb.Flight
		url := fmt.Sprintf("%s/api/v0.1/pods/%s/flights/%s", r.baseUrl, podName, flight)
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

	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
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
