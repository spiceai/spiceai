package registry

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/github"
	"github.com/spiceai/spice/pkg/spec"
	"gopkg.in/yaml.v2"
)

type GitHubRegistry struct{}

const (
	gitHubRegistryOwner string = "spiceai"
	gitHubRegistryRepo  string = "registry"
)

func (r *GitHubRegistry) GetPod(podPath string) (string, error) {
	err := privatePreviewTokenCheck()
	if err != nil {
		return "", err
	}
	
	podName := strings.ToLower(filepath.Base(podPath))
	gh := github.NewGitHubClient(gitHubRegistryOwner, gitHubRegistryRepo, "")

	path := fmt.Sprintf("pods/%s", podPath)
	contents, err := github.GetContents(gh, path)
	if err != nil {
		var githubCallError *github.GitHubCallError
		if errors.As(err, &githubCallError); githubCallError.StatusCode == 404 {
			return "", NewRegistryItemNotFound(err)
		}
		return "", err
	}

	podManifestFileName := fmt.Sprintf("%s.yaml", podName)

	var podManifest *github.RepoContent
	for _, c := range contents {
		if c.Size > 0 && c.Type == "file" && c.Name == podManifestFileName {
			podManifest = &c
			break
		}
	}

	if podManifest == nil {
		return "", errors.New("pod not found")
	}

	downloadPath := filepath.Join(config.PodsManifestsPath(), podManifestFileName)

	err = os.MkdirAll(config.PodsManifestsPath(), 0766)
	if err != nil {
		return "", err
	}

	return downloadPath, gh.DownloadFile(podManifest.DownloadURL, downloadPath)
}

func (r *GitHubRegistry) GetDataSource(datasourcePath string) (*spec.DataSourceSpec, error) {
	err := privatePreviewTokenCheck()
	if err != nil {
		return nil, err
	}

	gh := github.NewGitHubClient(gitHubRegistryOwner, gitHubRegistryRepo, "")

	path := fmt.Sprintf("datasources/%s", datasourcePath)
	contents, err := github.GetContents(gh, path)
	if err != nil {
		var githubCallError *github.GitHubCallError
		if errors.As(err, &githubCallError); githubCallError.StatusCode == 404 {
			return nil, NewRegistryItemNotFound(err)
		}
		return nil, err
	}

	dataSourceName := strings.ToLower(filepath.Base(datasourcePath))
	dataSourceFileName := fmt.Sprintf("%s.yaml", dataSourceName)

	var dataSourceContent *github.RepoContent
	for _, c := range contents {
		if c.Size > 0 && c.Type == "file" && c.Name == dataSourceFileName {
			dataSourceContent = &c
			break
		}
	}

	if dataSourceContent == nil {
		return nil, errors.New("datasource not found")
	}

	data, err := gh.Get(dataSourceContent.DownloadURL, nil)
	if err != nil {
		return nil, err
	}

	var dataSource spec.DataSourceSpec
	err = yaml.Unmarshal(data, &dataSource)
	if err != nil {
		return nil, fmt.Errorf("invalid datasource %w", err)
	}

	return &dataSource, nil
}

func privatePreviewTokenCheck() error {
	ghToken := os.Getenv("SPICE_GH_TOKEN") != ""
	if !ghToken {
		return errors.New("SPICE_GH_TOKEN is required to be set during Private Preview. See https://github.com/spiceai/spiceai#prerequisites-developer-preview-only")
	}
	return nil
}