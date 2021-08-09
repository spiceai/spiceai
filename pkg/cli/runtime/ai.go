package runtime

import (
	"errors"
	"os"
	"path"

	"github.com/spiceai/spice/pkg/config"
)

func EnsureAIPresent() error {
	aiInstallPath := config.AiEnginePath()

	if _, err := os.Stat(aiInstallPath); !os.IsNotExist(err) {
		if err != nil {
			return err
		}
		return nil
	}

	spiceRepoRoot := os.Getenv("SPICE_REPO_ROOT")
	if spiceRepoRoot == "" {
		return errors.New("SPICE_REPO_ROOT is unset. Please ensure it contains the full path to the root of your spiceai repo")
	}

	targetPath := path.Join(spiceRepoRoot, "ai", "src")

	err := os.Symlink(targetPath, aiInstallPath)
	if err != nil {
		return err
	}

	return nil
}
