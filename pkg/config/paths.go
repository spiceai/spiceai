package config

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/context"
)

const (
	dockerAppPath          string = "/userapp"
	dockerSpiceRuntimePath string = "/.spice"
	dockerAiEnginePath     string = "/app/ai"
)

var (
	spiceRuntimePath string
	spiceBinPath     string
	spiceLogPath     string
	aiEnginePath     string
	appPath          string
	appSpicePath     string
	podManifestPath  string
)

func SpiceRuntimePath() string {
	if context.CurrentContext() == context.Docker {
		return dockerSpiceRuntimePath
	}

	if spiceRuntimePath == "" {
		homeDir := os.Getenv("HOME")
		spiceRuntimePath = path.Join(homeDir, ".spice")
	}
	return spiceRuntimePath
}

func SpiceBinPath() string {
	if spiceBinPath == "" {
		spiceBinPath = filepath.Join(SpiceRuntimePath(), "bin")
	}
	return spiceBinPath
}

func SpiceLogPath() string {
	if spiceLogPath == "" {
		spiceLogPath = filepath.Join(SpiceRuntimePath(), "log")
	}
	return spiceLogPath
}

func AiEnginePath() string {
	if context.CurrentContext() == context.Docker {
		return dockerAiEnginePath
	}

	if aiEnginePath == "" {
		aiEnginePath = path.Join(SpiceBinPath(), "ai")
	}
	return aiEnginePath
}

func AppPath() string {
	if context.CurrentContext() == context.Docker {
		return dockerAppPath
	}

	if appPath == "" {
		cwd, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		appPath = cwd
	}

	return appPath
}

func AppSpicePath() string {
	if appSpicePath == "" {
		appSpicePath = path.Join(AppPath(), ".spice")
	}
	return appSpicePath
}

func PodsManifestsPath() string {
	if podManifestPath == "" {
		podManifestPath = path.Join(AppSpicePath(), "pods")
	}
	return podManifestPath
}

func GetSpiceAppRelativePath(absolutePath string) string {
	if strings.HasPrefix(absolutePath, AppPath()) {
		return absolutePath[len(AppPath())+1:]
	}
	return absolutePath
}
