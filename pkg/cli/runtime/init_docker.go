package runtime

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/config"
)

const (
	DockerVersionFileName = "spiced_docker_version.txt"
)

func GetDockerVersionFilePath() string {
	return filepath.Join(config.SpiceBinPath(), DockerVersionFileName)
}

func initDocker() error {
	dockerRuntimeVersionFile := GetDockerVersionFilePath()

	// Don't upgrade the docker image if we are pointing to "dev"
	if _, err := os.Stat(dockerRuntimeVersionFile); err == nil {
		dockerVersion, err := os.ReadFile(dockerRuntimeVersionFile)
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(dockerVersion)) == "dev" {
			return nil
		}
	}

	release, err := getLatestRuntimeRelease()
	if err != nil {
		return err
	}

	runtimeVersion := trimSuffix(release.TagName)

	// Write this to file
	return os.WriteFile(dockerRuntimeVersionFile, []byte(runtimeVersion), 0766)
}
