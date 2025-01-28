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
	"io"
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

func DownloadRuntimeAsset(flavor constants.Flavor, release *RepoRelease, downloadPath string, allowAccelerator bool) error {
	assetName := GetRuntimeAssetName(flavor, allowAccelerator)
	slog.Info(fmt.Sprintf("Downloading the Spice runtime..., %s", assetName))
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func DownloadAsset(release *RepoRelease, downloadPath string, assetName string) error {
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func GetRuntimeAssetName(flavor constants.Flavor, allowAccelerator bool) string {
	var downloadFlavor string
	if flavor == constants.FlavorAI || flavor == constants.FlavorDefault {
		if accelerator, exists := get_ai_accelerator(); exists && allowAccelerator {
			downloadFlavor = fmt.Sprintf("_models_%s", accelerator)
		} else {
			downloadFlavor = "_models"
		}
	}

	assetName := fmt.Sprintf("%s%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, downloadFlavor, runtime.GOOS, getRustArch())

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

// GPU versions that are supported via dedicated CUDA builds
var supportedCudaVersionsBinaries = []string{"80", "86", "87", "89", "90"}

func checkCudaVersionSupported(computeCap string) bool {
	for _, version := range supportedCudaVersionsBinaries {
		if computeCap == version {
			return true
		}
	}
	return false
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

	if runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		version, err := get_cuda_version()
		if err != nil {
			slog.Error("checking for CUDA device", "error", err)
		}

		if version == nil {
			return "", false
		}

		if !checkCudaVersionSupported(*version) {
			slog.Warn(fmt.Sprintf("Spice detected a GPU, but the GPU version (%s) is not supported for model acceleration. Spice will fallback to using the CPU to run local models, which may impact performance.", *version))
			return "", false
		}

		return "cuda_" + *version, true
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

func get_cuda_version() (*string, error) {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		return nil, nil
	}

	slog.Debug("Running `nvidia-smi --query-gpu=compute_cap --format=csv,noheader` to determine hardware")
	cmd := exec.Command("nvidia-smi", "--query-gpu=compute_cap", "--format=csv,noheader")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start `nvidia-smi` command: %w", err)
	}

	// Read the output while the command is still running
	cmdOutput, readErr := io.ReadAll(stdout)

	waitErr := cmd.Wait()

	// If `nvidia-smi` exits with a non-zero status, treat it as no GPU available
	if waitErr != nil {
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			slog.Warn("`nvidia-smi` command failed", "exit_code", exitErr.ExitCode(), "error", exitErr)
			return nil, nil
		}
		return nil, fmt.Errorf("unexpected error while waiting for `nvidia-smi`: %w", waitErr)
	}

	// Handle output reading errors separately
	if readErr != nil {
		return nil, fmt.Errorf("failed to read output: %w", readErr)
	}

	// Get CUDA version, if available: e.g., "8.6" will be returned as "86"
	version := strings.ReplaceAll(strings.TrimSpace(string(cmdOutput)), ".", "")

	if version == "" {
		return nil, nil
	}

	return &version, nil
}
