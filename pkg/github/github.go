package github

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spiceai/spiceai/pkg/util"
)

type GitHubClient struct {
	Owner string
	Repo  string
	Token string
}

func NewGitHubClientFromPath(path string) (*GitHubClient, error) {
	gitHubPathSplit := strings.Split(path, "/")

	if gitHubPathSplit[0] != "github.com" {
		return nil, fmt.Errorf("invalid configuration! unknown path: %s", path)
	}

	owner := gitHubPathSplit[1]
	repo := gitHubPathSplit[2]

	return NewGitHubClient(owner, repo), nil
}

func NewGitHubClient(owner string, repo string) *GitHubClient {
	return &GitHubClient{
		Owner: owner,
		Repo:  repo,
	}
}

func (g *GitHubClient) Get(url string, payload []byte) ([]byte, error) {
	return g.call("GET", url, payload, "application/vnd.github.v3+json")
}

func (g *GitHubClient) DownloadFile(url string, downloadPath string) error {
	body, err := g.Get(url, nil)
	if err != nil {
		return err
	}

	return os.WriteFile(downloadPath, body, 0766)
}

func (g *GitHubClient) DownloadTarGzip(url string, downloadDir string) error {
	body, err := g.Get(url, nil)
	if err != nil {
		return err
	}

	return util.ExtractTarGz(body, downloadDir)
}

func (g *GitHubClient) call(method string, url string, payload []byte, accept string) ([]byte, error) {
	if payload == nil {
		payload = make([]byte, 0)
	}

	payloadReader := bytes.NewReader(payload)

	req, err := http.NewRequest(method, url, payloadReader)
	if err != nil {
		return nil, err
	}

	if g.Token != "" {
		req.Header.Add("Authorization", fmt.Sprintf("token %s", g.Token))
	}

	if accept != "" {
		req.Header.Add("Accept", accept)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, NewGitHubCallError(fmt.Sprintf("Error calling GitHub: %s", string(body)), response.StatusCode)
	}

	return body, nil
}
