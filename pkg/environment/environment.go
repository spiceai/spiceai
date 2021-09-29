package environment

import (
	"context"

	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/state"
	"golang.org/x/sync/errgroup"
)

var initCompleted bool = false

func InitializationCompleted() bool {
	return initCompleted
}

func InitDataConnectors() error {
	errGroup, _ := errgroup.WithContext(context.Background())
	for _, pod := range pods.Pods() {
		p := pod
		errGroup.Go(func() error {
			return InitPodDataConnector(p)
		})
	}
	err := errGroup.Wait()
	initCompleted = true
	return err
}

func InitPodDataConnector(pod *pods.Pod) error {
	handler := func(state *state.State, metadata map[string]string) error {
		return aiengine.SendData(pod, state)
	}
	err := pod.InitDataConnectors(handler)
	if err != nil {
		return err
	}

	return nil
}
