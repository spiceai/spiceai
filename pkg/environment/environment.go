package environment

import (
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/state"
)

func RegisterStateHandlers() {
	for _, pod := range pods.Pods() {
		handler := func(state *state.State, metadata map[string]string) error {
			return aiengine.SendData(pod, state)
		}
		pod.RegisterStateHandler(handler)
	}
}
