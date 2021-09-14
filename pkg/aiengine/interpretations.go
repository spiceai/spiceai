package aiengine

import (
	"context"
	"fmt"
	"time"

	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
)

func sendInterpretations(pod *pods.Pod, indexedInterpretations *common_pb.IndexedInterpretations) error {
	if len(indexedInterpretations.Interpretations) == 0 {
		// Nothing to do
		return nil
	}

	err := IsServerHealthy()
	if err != nil {
		return err
	}

	addInterpretationsRequest := &aiengine_pb.AddInterpretationsRequest{
		Pod:                    pod.Name,
		IndexedInterpretations: indexedInterpretations,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.AddInterpretations(ctx, addInterpretationsRequest)
	if err != nil {
		return fmt.Errorf("failed to post new interpretations to pod %s: %w", pod.Name, err)
	}

	if response.Error {
		return fmt.Errorf("failed to post new interpretations to pod %s: %s", pod.Name, response.Result)
	}

	return nil
}
