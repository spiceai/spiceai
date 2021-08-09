package api

import "github.com/spiceai/spice/pkg/pods"

type Pod struct {
	Name         string `json:"name"`
	ManifestPath string `json:"manifest_path"`
}

func NewPod(f *pods.Pod) *Pod {
	return &Pod{
		Name:         f.Name,
		ManifestPath: f.ManifestPath(),
	}
}
