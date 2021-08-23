package runtime

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"

	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/github"
	"golang.org/x/mod/semver"
)

var (
	assetNameMemo string
	githubClient  *github.GitHubClient
)

const (
	runtimeFileName = "spiced"
	runtimeOwner    = "spiceai"
	runtimeRepo     = "spiceai"
)

// Init installs Spice on a local machine using the supplied runtimeVersion.
func Init(cliContext context.RuntimeContext) error {
	githubToken := github.GetGitHubTokenFromEnv()
	githubClient = github.NewGitHubClient(runtimeOwner, runtimeRepo, githubToken)

	switch cliContext {
	case context.Docker:
		return initDocker()
	case context.BareMetal:
		return initBareMetal()
	default:
		return fmt.Errorf("unknown context: %v", cliContext)
	}
}

func getLatestRuntimeRelease() (*github.RepoRelease, error) {
	fmt.Println("Checking for latest Spice runtime release...")

	releases, err := github.GetReleases(githubClient)
	if err != nil {
		return nil, err
	}

	if len(releases) == 0 {
		return nil, errors.New("no releases found")
	}

	// Sort by semver in descending order
	sort.Sort(RepoReleases(releases))

	release := getRuntimeRelease(releases)

	if release == nil {
		return nil, errors.New("no releases found")
	}

	return release, nil
}

type RepoReleases []github.RepoRelease

func (r RepoReleases) Len() int {
	return len(r)
}

func (r RepoReleases) Less(i, j int) bool {
	one := r[i]
	two := r[j]

	// Compare the releases via a semver comparison in descending order
	return semver.Compare(one.TagName, two.TagName) == 1
}

func (r RepoReleases) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func trimSuffix(tagName string) string {
	return strings.TrimSuffix(tagName, fmt.Sprintf("-%s", runtimeFileName))
}

func getRuntimeRelease(releases []github.RepoRelease) *github.RepoRelease {
	for _, release := range releases {
		for _, asset := range release.Assets {
			if asset.Name == getAssetName() {
				return &release
			}
		}
	}

	return nil
}

func getAssetName() string {
	if assetNameMemo != "" {
		return assetNameMemo
	}

	assetName := fmt.Sprintf("%s_%s_%s.tar.gz", runtimeFileName, runtime.GOOS, runtime.GOARCH)

	// TODO: ARM64 workaround
	if runtime.GOARCH == "arm64" {
		assetName = fmt.Sprintf("%s_%s_%s.tar.gz", runtimeFileName, runtime.GOOS, "amd64")
	}

	assetNameMemo = assetName
	return assetName
}
