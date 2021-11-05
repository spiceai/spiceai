package api

import (
	"github.com/spiceai/spiceai/pkg/pods"
)

type Pod struct {
	Name         string   `json:"name,omitempty" csv:"name"`
	ManifestPath string   `json:"manifest_path,omitempty" csv:"manifest_path"`
	Episodes     int      `json:"episodes,omitempty" csv:"episodes"`
	Measurements []string `json:"measurements,omitempty" csv:"-"`
	Categories   []string `json:"categories,omitempty" csv:"-"`
}

func NewPod(f *pods.Pod) *Pod {
	return &Pod{
		Name:         f.Name,
		Episodes:     f.Episodes(),
		ManifestPath: f.ManifestPath(),
		Measurements: f.MeasurementNames(),
		Categories:   f.CategoryNames(),
	}
}
