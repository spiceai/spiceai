package registry

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/spec"
	"gopkg.in/yaml.v2"
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

func (r *LocalFileRegistry) GetDataSource(datasourcePath string) (*spec.DataSourceSpec, error) {
	data, err := ioutil.ReadFile(datasourcePath)
	if err != nil {
		return nil, fmt.Errorf("datasource not found at %s: %w", datasourcePath, err)
	}

	var dataSource spec.DataSourceSpec
	err = yaml.Unmarshal(data, &dataSource)
	if err != nil {
		return nil, fmt.Errorf("Invalid datasource %w", err)
	}

	return &dataSource, nil
}
