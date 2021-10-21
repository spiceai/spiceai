package api

import (
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

func NewPod(f *pods.Pod) *runtime_pb.Pod {
	return &runtime_pb.Pod{
		Name:         f.Name,
		ManifestPath: f.ManifestPath(),
		Measurements: f.MeasurementNames(),
		Categories:   f.CategoryNames(),
	}
}
