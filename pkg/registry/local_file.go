package registry

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/spiceai/spice/pkg/config"
)

type LocalFileRegistry struct{}

func (r *LocalFileRegistry) GetPod(podPath string) (string, error) {
	input, err := ioutil.ReadFile(podPath)
	if err != nil {
		return "", fmt.Errorf("pod not found at %s: %w", podPath, err)
	}

	podManifestFileName := filepath.Base(podPath)

	podManifestPath := filepath.Join(config.PodsManifestsPath(), podManifestFileName)

	err = ioutil.WriteFile(podManifestPath, input, 0644)
	if err != nil {
		return "", fmt.Errorf("error fetching pod %s", podPath)
	}

	return podManifestPath, nil
}
