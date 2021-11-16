package github

import (
	"fmt"
	"runtime"

	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/version"
)

var (
	assetNameMemo string
	githubClient  = NewGitHubClient(runtimeOwner, runtimeRepo)
)

const (
	runtimeOwner = "spiceai"
	runtimeRepo  = "spiceai"
)

func CheckForLatestVersion() {
	release, err := GetLatestCliRelease()
	if err != nil {
		return
	}
	cliVersion := version.Version()
	if cliVersion != release.TagName {
		fmt.Printf("Note: New CLI version %s is now available!\nNote: Run \"spice upgrade\" to update CLI \n", release.TagName)
		return
	}
}

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
