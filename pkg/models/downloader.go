package models

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/github"
	"github.com/spiceai/spice/pkg/models/downloaders"
)

type ModelDownloader interface {
	Fetch() (string, error)
}

func GetDownloader(podName string, connectionId string, connection config.ConnectionSpec, branch string) (ModelDownloader, error) {
	var downloader ModelDownloader = nil
	var err error
	switch connectionId {
	case "github":
		downloader, err = NewGitHubDownloader(podName, connection.Name, os.Getenv(connection.Token), branch)
	default:
		err = fmt.Errorf("unknown connection %s", connectionId)
	}

	if err != nil {
		return nil, err
	}

	return downloader, nil
}

func NewGitHubDownloader(pod string, githubPath string, token string, branch string) (ModelDownloader, error) {
	manifestsPath := config.PodsManifestsPath()
	saveFolder := filepath.Join(manifestsPath, pod, "models")
	err := os.MkdirAll(saveFolder, 0766)
	if err != nil {
		return nil, err
	}

	workflowPath := fmt.Sprintf(".github/workflows/%s.yml", pod)

	gitHubClient, err := github.NewGitHubClientFromPath(githubPath, token)
	if err != nil {
		return nil, err
	}

	return &downloaders.GitHubDownloader{
		GitHub:        gitHubClient,
		Branch:        branch,
		WorkflowPath:  workflowPath,
		PodName:       pod,
		ModelSavePath: saveFolder,
	}, nil
}
