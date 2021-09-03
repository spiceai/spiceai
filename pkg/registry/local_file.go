package registry

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

type LocalFileRegistry struct{}

func (r *LocalFileRegistry) GetPod(podPath string) (string, error) {
	input, err := ioutil.ReadFile(podPath)
	if err != nil {
		return "", fmt.Errorf("pod not found at %s: %w", podPath, err)
	}

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

	podManifestFileName := filepath.Base(podPath)

	podManifestPath := filepath.Join(context.CurrentContext().PodsDir(), podManifestFileName)

	err = ioutil.WriteFile(podManifestPath, input, 0644)
	if err != nil {
		return "", fmt.Errorf("error fetching pod %s", podPath)
	}

	return podManifestPath, nil
}
