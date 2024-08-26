/*
Copyright 2024 The Spice.ai OSS Authors

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
	"runtime"

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
	fmt.Println("Checking for latest Spice runtime release...")

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
	fmt.Println("Downloading the Spice runtime...", assetName)
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func DownloadAsset(release *RepoRelease, downloadPath string, assetName string) error {
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func GetRuntimeAssetName(flavor string) string {
	if flavor != "" {
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
