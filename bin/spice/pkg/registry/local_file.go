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

package registry

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type LocalFileRegistry struct{}

func (r *LocalFileRegistry) GetPod(ctx *context.RuntimeContext, podPath string) (string, error) {
	stat, err := os.Stat(podPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("the Spicepod directory '%s' does not exist", podPath)
		}
		return "", fmt.Errorf("spicepod.yaml not found at %s: %w", podPath, err)
	}

	if !stat.IsDir() {
		return "", fmt.Errorf("expected '%s' to be a directory", podPath)
	}

	if !filepath.IsAbs(podPath) {
		podPath, err = filepath.Abs(podPath)
		if err != nil {
			return "", fmt.Errorf("error fetching Spicepod '%s': %w", podPath, err)
		}
	}

	// Validate source
	podManifestFileName := fmt.Sprintf("%s.yaml", strings.ToLower(filepath.Base(podPath)))
	podManifestPath := filepath.Join(ctx.PodsDir(), podManifestFileName)

	if _, err := os.Stat(filepath.Join(podPath, podManifestFileName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("the directory '%s' does not contain a spicepod.yaml. Is it a valid Spicepod?", podPath)
		}
		return "", fmt.Errorf("error fetching Spicepod %s: %w", podPath, err)
	}

	// Prepare destination
	podsDir := ctx.PodsDir()
	if _, err = os.Stat(podsDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("error fetching Spicepod %s: %w", podPath, err)
		}
		_, err = util.MkDirAllInheritPerm(podsDir)
		if err != nil {
			return "", fmt.Errorf("error fetching Spicepod %s: %w", podPath, err)
		}
	}

	// Copy files
	fileList := make(map[string]fs.DirEntry)
	err = filepath.WalkDir(podPath, func(path string, d fs.DirEntry, err error) error {
		if path != podPath {
			fileList[path] = d
		}
		return err
	})
	if err != nil {
		return "", fmt.Errorf("error fetching Spicepod %s: %w", podPath, err)
	}

	if len(fileList) == 0 {
		return "", fmt.Errorf("the directory '%s' is empty", podPath)
	}

	for path, d := range fileList {
		dst := filepath.Join(podsDir, strings.TrimPrefix(path, podPath))
		if d.IsDir() {
			if err := os.Mkdir(dst, stat.Mode().Perm()); err != nil {
				if !errors.Is(err, os.ErrExist) {
					return "", fmt.Errorf("error creating directory '%s'", dst)
				}
			}
			continue
		}

		err := util.CopyFile(path, dst)
		if err != nil {
			return "", fmt.Errorf("error copying file '%s' to '%s'", path, dst)
		}
	}

	return podManifestPath, nil
}
