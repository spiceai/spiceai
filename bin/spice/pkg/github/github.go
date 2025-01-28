/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type GitHubClient struct {
	token string
	Owner string
	Repo  string
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
	token := os.Getenv("GH_TOKEN")
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}

	return &GitHubClient{
		token: token,
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

	if accept != "" {
		req.Header.Add("Accept", accept)
	}

	// Add Authorization header if GITHUB_TOKEN is present
	if g.token != "" {
		req.Header.Add("Authorization", "Bearer "+g.token)
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

	if response.StatusCode == 401 {
		return nil, NewGitHubCallError("Detected GitHub token from GH_TOKEN or GITHUB_TOKEN environment variable is invalid. Check the token and try again.", response.StatusCode)
	}

	if response.StatusCode != 200 {
		return nil, NewGitHubCallError(fmt.Sprintf("Error calling GitHub: %s", string(body)), response.StatusCode)
	}

	return body, nil
}
