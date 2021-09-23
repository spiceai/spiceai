package environment

import (
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/state"
)

func InitDataConnectors() error {
	for _, pod := range pods.Pods() {
		handler := func(state *state.State, metadata map[string]string) error {
			return aiengine.SendData(pod, state)
		}
		err := pod.InitDataConnectors(handler)
		if err != nil {
			return err
		}
	}
	return nil
}
