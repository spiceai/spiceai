package aiengine

import (
	"context"
	"fmt"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
)

func sendInterpretations(pod *pods.Pod, indexedInterpretations *common_pb.IndexedInterpretations) error {
	zaplog.Sugar().Debugf("Sending %d interpretations to AI engine\n", aurora.BrightYellow(len(indexedInterpretations.Interpretations)))
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

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.AddInterpretations(ctx, addInterpretationsRequest)
	if err != nil {
		return fmt.Errorf("failed to post new interpretations to pod %s: %w", pod.Name, err)
	}

	if response.Error {
		return fmt.Errorf("failed to post new interpretations to pod %s: %s", pod.Name, response.Result)
	}

	duration := time.Since(startTime)
	zaplog.Sugar().Debugf("Sent %d interpretations to AI engine in %.2f seconds\n", len(indexedInterpretations.Interpretations), duration.Seconds())

	return nil
}
