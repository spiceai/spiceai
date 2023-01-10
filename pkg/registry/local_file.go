package registry

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

type LocalFileRegistry struct{}

func (r *LocalFileRegistry) GetPod(podPath string) (string, error) {
	stat, err := os.Stat(podPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("the pod directory '%s' does not exist", podPath)
		}
		return "", fmt.Errorf("pod not found at %s: %w", podPath, err)
	}

	if !stat.IsDir() {
		return "", fmt.Errorf("expected '%s' to be a directory", podPath)
	}

	if !filepath.IsAbs(podPath) {
		podPath, err = filepath.Abs(podPath)
		if err != nil {
			return "", fmt.Errorf("error fetching pod'%s': %w", podPath, err)
		}
	}

	// Validate source
	podManifestFileName := fmt.Sprintf("%s.yaml", strings.ToLower(filepath.Base(podPath)))
	podManifestPath := filepath.Join(context.CurrentContext().PodsDir(), podManifestFileName)

	if _, err := os.Stat(filepath.Join(podPath, podManifestFileName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("the directory '%s' does not contain a manifest. Is it a valid pod?", podPath)
		}
		return "", fmt.Errorf("error fetching pod %s: %w", podPath, err)
	}

	// Prepare destination
	podsDir := context.CurrentContext().PodsDir()
	if _, err = os.Stat(podsDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("error fetching pod %s: %w", podPath, err)
		}
		_, err = util.MkDirAllInheritPerm(podsDir)
		if err != nil {
			return "", fmt.Errorf("error fetching pod %s: %w", podPath, err)
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
		return "", fmt.Errorf("error fetching pod %s: %w", podPath, err)
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
