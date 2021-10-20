package aiengine

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
)

func importModel(pod *pods.Pod, tag string) error {
	modelName := fmt.Sprintf("%s_train", pod.Name)
	podDir := filepath.Dir(pod.ManifestPath())
	modelPath := filepath.Join(podDir, modelName)

	importRequest := &aiengine_pb.ImportModelRequest{
		Pod:        pod.Name,
		Tag:        tag,
		ImportPath: modelPath,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.ImportModel(ctx, importRequest)
	if err != nil {
		return err
	}

	if response.Error {
		return fmt.Errorf("%s: %s", response.Result, response.Message)
	}

	return nil
}
