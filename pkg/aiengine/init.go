package aiengine

import (
	"context"
	"fmt"
	"time"

	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/proto/aiengine_pb"
)

var podInitMap map[string]*aiengine_pb.InitRequest

func InitializePod(pod *pods.Pod) error {
	err := pod.ValidateForTraining()
	if err != nil {
		return err
	}

	podInit := getPodInitForTraining(pod)

	podInit.ActionsOrder = make(map[string]int32, len(podInit.Actions))

	var order int32 = 0
	for action := range podInit.Actions {
		podInit.ActionsOrder[action] = order
		order += 1
	}

	err = sendInit(podInit)
	if err != nil {
		return err
	}

	podInitMap[pod.Name] = podInit

	return nil
}

func sendInit(podInit *aiengine_pb.InitRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.Init(ctx, podInit)
	if err != nil {
		return err
	}

	if response.Error {
		return fmt.Errorf("failed to validate manifest: %s", response.Result)
	}

	return nil
}

func init() {
	podInitMap = make(map[string]*aiengine_pb.InitRequest)
}
