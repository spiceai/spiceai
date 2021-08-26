package api

import (
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/proto/runtime_pb"
)

func NewPod(f *pods.Pod) *runtime_pb.Pod {
	return &runtime_pb.Pod{
		Name:         f.Name,
		ManifestPath: f.ManifestPath(),
	}
}
