package github

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/spiceai/spice/pkg/constants"
)

var (
	assetNameMemo string
	githubClient  = NewGitHubClient(runtimeOwner, runtimeRepo, GetGitHubTokenFromEnv())
)

const (
	runtimeOwner = "spiceai"
	runtimeRepo  = "spiceai"
)

func GetLatestRuntimeRelease() (*RepoRelease, error) {
	fmt.Println("Checking for latest Spice runtime release...")

	releases, err := GetReleases(githubClient)
	if err != nil {
		return nil, err
	}

	if len(releases) == 0 {
		return nil, errors.New("no releases found")
	}

	// Sort by semver in descending order
	sort.Sort(releases)

	release := GetReleaseByAssetName(releases, getAssetName())

	if release == nil {
		return nil, errors.New("no releases found")
	}

	return release, nil
}

func GetRuntimeVersion(release *RepoRelease) string {
	return strings.TrimSuffix(release.TagName, fmt.Sprintf("-%s", constants.SpiceRuntimeFilename))
}

func DownloadRuntimeAsset(release *RepoRelease, downloadPath string) error {
	assetName := getAssetName()
	return DownloadPrivateReleaseAsset(githubClient, release, assetName, downloadPath)
}

func getAssetName() string {
	if assetNameMemo != "" {
		return assetNameMemo
	}

	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, runtime.GOOS, runtime.GOARCH)

	// TODO: ARM64 workaround
	if runtime.GOARCH == "arm64" {
		assetName = fmt.Sprintf("%s_%s_%s.tar.gz", constants.SpiceRuntimeFilename, runtime.GOOS, "amd64")
	}

	assetNameMemo = assetName
	return assetName
}
