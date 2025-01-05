/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spicepod

import (
	"fmt"
	"os"
	"path"

	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v3"
)

func CreateManifest(name string, spicepodDir string) (string, error) {
	fs, err := os.Stat(name)
	if err != nil {
		if os.IsNotExist(err) && spicepodDir != "." {
			err = os.Mkdir(name, 0766)
			if err != nil {
				return "", fmt.Errorf("Error creating directory: %w", err)
			}
			fs, err = os.Stat(name)
			if err != nil {
				return "", fmt.Errorf("Error checking if directory exists: %w", err)
			}
		} else if spicepodDir != "." {
			return "", fmt.Errorf("Error checking if directory exists: %w", err)
		} else {
			fs, err = os.Stat(spicepodDir)
			if err != nil {
				return "", fmt.Errorf("Error checking if directory exists: %w", err)
			}
		}
	}

	if !fs.IsDir() {
		return "", fmt.Errorf("Error: %s exists and is not a directory", name)
	}

	skeletonPod := &spec.SpicepodSpec{
		SpicepodSpecFields: spec.SpicepodSpecFields{
			Name:    name,
			Version: "v1",
			Kind:    "Spicepod",
		},
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
