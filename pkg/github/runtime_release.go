package github

import (
	"fmt"
	"runtime"

	"github.com/spiceai/spiceai/pkg/constants"
)

var (
	assetNameMemo string
	githubClient  = NewGitHubClient(runtimeOwner, runtimeRepo)
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
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}
func DownloadAsset(release *RepoRelease, downloadPath string, assetName string) error {
	return DownloadReleaseAsset(githubClient, release, assetName, downloadPath)
}
func GetRuntimeAssetName() string {
	if assetNameMemo != "" {
		return assetNameMemo
	}

	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, runtime.GOOS, runtime.GOARCH)

	assetNameMemo = assetName
	return assetName
}
func GetAssetName(assetFileName string) string {
	if assetNameMemo != "" {
		return assetNameMemo
	}

	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", assetFileName, runtime.GOOS, runtime.GOARCH)

	assetNameMemo = assetName
	return assetName
}
