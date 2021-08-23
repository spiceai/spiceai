package github

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/util"
)

type GitHubClient struct {
	Owner string
	Repo  string
	Token string
}

func NewGitHubClientFromPath(path string, token string) (*GitHubClient, error) {
	gitHubPathSplit := strings.Split(path, "/")

	if gitHubPathSplit[0] != "github.com" {
		return nil, fmt.Errorf("invalid configuration! unknown path: %s", path)
	}

	owner := gitHubPathSplit[1]
	repo := gitHubPathSplit[2]

	return NewGitHubClient(owner, repo, token), nil
}

func NewGitHubClient(owner string, repo string, token string) *GitHubClient {
	if token == "" {
		token = GetGitHubTokenFromEnv()
	}

	return &GitHubClient{
		Owner: owner,
		Repo:  repo,
		Token: token,
	}
}

func GetGitHubTokenFromEnv() string {
	token := os.Getenv("SPICE_GH_TOKEN")
	if token == "" {
		token = os.Getenv("GITHUB_TOKEN")
	}
	return token
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

	return g.ExtractTarGz(body, downloadDir)
}

func (g *GitHubClient) ExtractZip(body []byte, downloadDir string) error {
	zipBytesReader := bytes.NewReader(body)

	zipReader, err := zip.NewReader(zipBytesReader, int64(len(body)))
	if err != nil {
		return err
	}

	for _, file := range zipReader.File {
		reader, err := file.Open()
		if err != nil {
			return err
		}

		defer reader.Close()

		fileName := file.FileInfo().Name()

		fileToWrite := filepath.Join(downloadDir, fileName)

		newFile, err := os.Create(fileToWrite)
		if err != nil {
			return err
		}

		defer newFile.Close()

		_, err = io.Copy(newFile, reader)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *GitHubClient) ExtractTarGzInsideZip(body []byte, downloadDir string) error {
	zipBytesReader := bytes.NewReader(body)
	zipReader, err := zip.NewReader(zipBytesReader, int64(len(body)))
	if err != nil {
		return err
	}

	for _, file := range zipReader.File {
		reader, err := file.Open()
		if err != nil {
			return err
		}

		defer reader.Close()

		tarFileName := file.FileInfo().Name()

		if !strings.HasSuffix(tarFileName, ".tar.gz") {
			return errors.New("Unexpected file: " + tarFileName)
		}

		err = util.Untar(reader, downloadDir, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *GitHubClient) ExtractTarGz(body []byte, downloadDir string) error {
	bodyReader := bytes.NewReader(body)
	err := util.Untar(bodyReader, downloadDir, true)
	if err.Error() == "requires gzip-compressed body: gzip: invalid header" {
		_, err = bodyReader.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		return util.Untar(bodyReader, downloadDir, false)
	}
	return err
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
