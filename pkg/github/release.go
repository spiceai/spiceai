package github

import (
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
)

type RepoRelease struct {
	URL             string         `json:"url"`
	HTMLURL         string         `json:"html_url"`
	AssetsURL       string         `json:"assets_url"`
	UploadURL       string         `json:"upload_url"`
	TarballURL      string         `json:"tarball_url"`
	ZipballURL      string         `json:"zipball_url"`
	ID              int64          `json:"id"`
	NodeID          string         `json:"node_id"`
	TagName         string         `json:"tag_name"`
	TargetCommitish string         `json:"target_commitish"`
	Name            string         `json:"name"`
	Body            string         `json:"body"`
	Draft           bool           `json:"draft"`
	Prerelease      bool           `json:"prerelease"`
	CreatedAt       string         `json:"created_at"`
	PublishedAt     string         `json:"published_at"`
	Author          Author         `json:"author"`
	Assets          []ReleaseAsset `json:"assets"`
}

type RepoReleases []RepoRelease

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

func GetReleases(gh *GitHubClient) (RepoReleases, error) {
	releasesURL := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases", gh.Owner, gh.Repo)
	body, err := gh.Get(releasesURL, nil)
	if err != nil {
		return nil, err
	}

	var githubRepoReleases []RepoRelease
	err = json.Unmarshal(body, &githubRepoReleases)
	if err != nil {
		return nil, err
	}

	return githubRepoReleases, nil
}

func GetReleaseByAssetName(releases RepoReleases, assetName string) *RepoRelease {
	for _, release := range releases {
		for _, asset := range release.Assets {
			if asset.Name == assetName {
				return &release
			}
		}
	}

	return nil
}

func GetLatestReleaseTagName(gh *GitHubClient) (string, error) {
	githubRepoReleases, err := GetReleases(gh)
	if err != nil {
		return "", err
	}

	if len(githubRepoReleases) == 0 {
		return "", fmt.Errorf("no releases")
	}

	for _, release := range githubRepoReleases {
		if !strings.Contains(release.TagName, "-rc") {
			return release.TagName, nil
		}
	}

	return "", fmt.Errorf("no releases")
}

func DownloadReleaseByTagName(gh *GitHubClient, tagName string, downloadDir string, filename string) error {
	archiveExt := "tar.gz"

	releaseUrl := fmt.Sprintf(
		"https://github.com/%s/%s/releases/download/%s/%s.%s",
		gh.Owner,
		gh.Repo,
		tagName,
		filename,
		archiveExt)

	return gh.DownloadTarGzip(releaseUrl, downloadDir)
}
