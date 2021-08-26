package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/proto/runtime_pb"
	"github.com/spiceai/spice/pkg/util"
)

type RuntimeClient struct {
	runtimeConfig *config.SpiceConfiguration
	pod           *pods.Pod
	serverBaseUrl string
}

func NewRuntimeClient(podName string) (*RuntimeClient, error) {
	pod, runtimeConfig, err := GetPodAndConfiguration(podName)
	if err != nil {
		return nil, err
	}

	serverBaseUrl := runtimeConfig.ServerBaseUrl()

	return &RuntimeClient{
		runtimeConfig: runtimeConfig,
		pod:           pod,
		serverBaseUrl: serverBaseUrl,
	}, nil
}

func (r *RuntimeClient) ExportModel(exportModelPath *exportPathResult, tag string) error {
	err := util.IsRuntimeServerHealthy(r.serverBaseUrl, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to reach %s. is the spice runtime running? %w", r.serverBaseUrl, err)
	}

	exportRequest := &runtime_pb.ExportModel{
		Directory: exportModelPath.directory,
		Filename:  exportModelPath.filename,
	}

	exportRequestBytes, err := json.Marshal(&exportRequest)
	if err != nil {
		return err
	}

	exportModelUrl := fmt.Sprintf("%s/api/v0.1/pods/%s/models/%s/export", r.serverBaseUrl, r.pod.Name, tag)
	response, err := http.DefaultClient.Post(exportModelUrl, "application/json", bytes.NewReader(exportRequestBytes))
	if err != nil {
		return nil
	}

	if response.StatusCode != 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to export model: %s", string(body))
	}

	return nil
}

func (r *RuntimeClient) ImportModel(archivePath string, tag string) error {
	err := util.IsRuntimeServerHealthy(r.serverBaseUrl, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to reach %s. is the spice runtime running? %w", r.serverBaseUrl, err)
	}

	importRequest := &runtime_pb.ImportModel{
		ArchivePath: archivePath,
		Tag:         tag,
		Pod:         r.pod.Name,
	}

	importRequestBytes, err := json.Marshal(&importRequest)
	if err != nil {
		return err
	}

	importModelUrl := fmt.Sprintf("%s/api/v0.1/pods/%s/models/%s/import", r.serverBaseUrl, r.pod.Name, tag)
	response, err := http.DefaultClient.Post(importModelUrl, "application/json", bytes.NewReader(importRequestBytes))
	if err != nil {
		return nil
	}

	if response.StatusCode != 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("failed to import model: %s", string(body))
	}

	return nil
}

func (r *RuntimeClient) StartTraining() error {
	err := util.IsRuntimeServerHealthy(r.serverBaseUrl, http.DefaultClient)
	if err != nil {
		return fmt.Errorf("failed to reach %s. is the spice runtime running? %w", r.serverBaseUrl, err)
	}

	trainUrl := fmt.Sprintf("%s/api/v0.1/pods/%s/train", r.serverBaseUrl, r.pod.Name)
	response, err := http.DefaultClient.Post(trainUrl, "application/json", nil)
	if err != nil {
		return fmt.Errorf("failed to start training: %w", err)
	}

	if response.StatusCode != 200 {
		return fmt.Errorf("failed to start training: %s", response.Status)
	}

	return nil
}
