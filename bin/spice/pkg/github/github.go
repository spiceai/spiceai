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
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// progressReader wraps an io.Reader and reports progress
type progressReader struct {
	reader    io.Reader
	total     int64
	current   int64
	lastPrint time.Time
	startTime time.Time
}

func newProgressReader(reader io.Reader, total int64) *progressReader {
	now := time.Now()
	return &progressReader{
		reader:    reader,
		total:     total,
		lastPrint: now,
		startTime: now,
	}
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	pr.current += int64(n)

	// Print progress every 500ms or on completion
	now := time.Now()
	if now.Sub(pr.lastPrint) > 500*time.Millisecond || err == io.EOF {
		pr.printProgress()
		pr.lastPrint = now
	}

	return n, err
}

func (pr *progressReader) printProgress() {
	if pr.total <= 0 {
		return
	}

	percent := float64(pr.current) / float64(pr.total) * 100
	downloaded := float64(pr.current) / 1024 / 1024 // MB
	total := float64(pr.total) / 1024 / 1024        // MB

	elapsed := time.Since(pr.startTime).Seconds()
	speed := downloaded / elapsed // MB/s

	// Calculate ETA
	remaining := total - downloaded
	eta := ""
	if speed > 0 {
		etaSeconds := remaining / speed
		if etaSeconds < 60 {
			eta = fmt.Sprintf("ETA: %ds", int(etaSeconds))
		} else {
			eta = fmt.Sprintf("ETA: %dm%ds", int(etaSeconds)/60, int(etaSeconds)%60)
		}
	}

	fmt.Fprintf(os.Stderr, "\rDownloading: %.1f%% (%.1f/%.1f MB) @ %.1f MB/s %s",
		percent, downloaded, total, speed, eta)

	if pr.current >= pr.total {
		fmt.Fprintln(os.Stderr) // New line on completion
	}
}

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

	// Use 0644 (rw-r--r--) instead of 0766 to prevent world-writable files
	return os.WriteFile(downloadPath, body, 0644)
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

	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
	}()

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

// callWithProgress makes an HTTP call and shows download progress
func (g *GitHubClient) callWithProgress(method string, url string, payload []byte, accept string, totalSize int64) ([]byte, error) {
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

	// Create a custom client that preserves headers on redirect
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			// Preserve Accept header on redirects
			if accept != "" {
				req.Header.Set("Accept", accept)
			}
			// Preserve Authorization header on redirects to GitHub domains
			if g.token != "" && len(via) > 0 {
				if strings.Contains(req.URL.Host, "github.com") || strings.Contains(req.URL.Host, "githubusercontent.com") {
					req.Header.Set("Authorization", "Bearer "+g.token)
				}
			}
			return nil
		},
	}

	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
	}()

	if response.StatusCode == 401 {
		return nil, NewGitHubCallError("Detected GitHub token from GH_TOKEN or GITHUB_TOKEN environment variable is invalid. Check the token and try again.", response.StatusCode)
	}

	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		return nil, NewGitHubCallError(fmt.Sprintf("Error calling GitHub: %s", string(body)), response.StatusCode)
	}

	// Wrap the response body with progress tracking
	progressReader := newProgressReader(response.Body, totalSize)
	body, err := io.ReadAll(progressReader)
	if err != nil {
		return nil, err
	}

	return body, nil
}
