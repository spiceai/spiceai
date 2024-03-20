package spicepod

import (
	"fmt"
	"os"
	"path"

	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v2"
)

func CreateManifest(name string, spicepodDir string) (string, error) {
	fs, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(name, 0766)
			if err != nil {
				return "", fmt.Errorf("Error creating directory: %w", err)
			}
			fs, err = os.Stat(name)
			if err != nil {
				return "", fmt.Errorf("Error checking if directory exists: %w", err)
			}
		} else {
			return "", fmt.Errorf("Error checking if directory exists: %w", err)
		}
	}

	if !fs.IsDir() {
		return "", fmt.Errorf("Error: %s exists and is not a directory", name)
	}

	skeletonPod := &spec.SpicepodSpec{
		Name:    name,
		Version: "v1beta1",
		Kind:    "Spicepod",
	}

	skeletonPodContentBytes, err := yaml.Marshal(skeletonPod)
	if err != nil {
		return "", fmt.Errorf("Error marshalling spicepod: %w", err)
	}

	spicepodPath := path.Join(spicepodDir, "spicepod.yaml")
	err = os.WriteFile(spicepodPath, skeletonPodContentBytes, 0744)
	if err != nil {
		return "", fmt.Errorf("Error writing spicepod.yaml: %w", err)
	}

	return spicepodPath, nil
}
