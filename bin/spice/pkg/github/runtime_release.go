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

func DownloadRuntimeAsset(release *RepoRelease, downloadPath string) error {
	assetName := GetRuntimeAssetName()
	fmt.Println("Downloading the Spice runtime...", assetName)
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func DownloadAsset(release *RepoRelease, downloadPath string, assetName string) error {
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}

func GetRuntimeAssetName() string {
	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, runtime.GOOS, getRustArch())

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
