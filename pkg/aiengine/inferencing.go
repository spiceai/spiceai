package aiengine

import (
	"context"
	"fmt"
	"time"

	"github.com/spiceai/spice/pkg/proto/aiengine_pb"
)

func Infer(pod string, tag string) (*aiengine_pb.InferenceResult, error) {
	if !ServerReady() {
		return nil, fmt.Errorf("not ready")
	}

	request := &aiengine_pb.InferenceRequest{
		Pod: pod,
		Tag: tag,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := aiengineClient.GetInference(ctx, request)
	if err != nil {
		return nil, err
	}

	return response, nil
}
