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

package github

import (
	"fmt"
	"log/slog"
	"os/exec"
	"runtime"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
)

var (
	githubClient = NewGitHubClient(runtimeOwner, runtimeRepo)
)

const (
	runtimeOwner = "spiceai"
	runtimeRepo  = "spiceai"
)

func GetLatestRuntimeRelease() (*RepoRelease, error) {
	release, err := GetLatestRelease(githubClient, GetAssetName(constants.SpiceRuntimeFilename))
	if err != nil {
		return nil, err
	}

	return release, nil
}

func GetLatestCliRelease() (*RepoRelease, error) {
	release, err := GetLatestRelease(githubClient, GetAssetName(constants.SpiceCliFilename))
	if err != nil {
		return nil, err
	}

	return release, nil
}

func DownloadRuntimeAsset(flavor string, release *RepoRelease, downloadPath string) error {
	assetName := GetRuntimeAssetName(flavor)
	slog.Info(fmt.Sprintf("Downloading the Spice runtime..., %s", assetName))
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func DownloadAsset(release *RepoRelease, downloadPath string, assetName string) error {
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func GetRuntimeAssetName(flavor string) string {
	switch {
	case flavor == "ai":
		if accelerator, exists := get_ai_accelerator(); exists {
			flavor = fmt.Sprintf("_%s_models", accelerator)
		} else {
			flavor = "_models"
		}
	case flavor != "":
		flavor = fmt.Sprintf("_%s", flavor)
	}

	assetName := fmt.Sprintf("%s%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, flavor, runtime.GOOS, getRustArch())

	return assetName
}

func GetAssetName(assetFileName string) string {
	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", assetFileName, runtime.GOOS, getRustArch())

	return assetName
}

func getRustArch() string {
	switch runtime.GOARCH {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	}
	return runtime.GOARCH
}

// get_ai_accelerator checks for accelerator devices, either GPU devices, or Apple silicon (metal).
func get_ai_accelerator() (string, bool) {
	if runtime.GOOS == "darwin" {
		hasMetal, err := has_metal_device()
		if err != nil {
			slog.Error("checking for metal device", "error", err)
		}
		if hasMetal {
			return "metal", true
		}
	}
	return "", false
}

// has_metal_device checks if the system is running on Apple silicon (metal) via the `system_profiler` command.
// For non-darwin systems, it does not attempt a `system_profiler` command.
func has_metal_device() (bool, error) {
	if runtime.GOOS != "darwin" {
		return false, nil
	}

	slog.Debug("On MacOs, running `system_profiler SPDisplaysDataType -detailLevel mini` to determine hardware")

	output, err := exec.Command("system_profiler", "SPDisplaysDataType", "-detailLevel", "mini").Output()
	if err != nil {
		return false, fmt.Errorf("failed to run system_profiler: %w", err)
	}
	return strings.Contains(string(output), "Metal Support: Metal"), nil
}
