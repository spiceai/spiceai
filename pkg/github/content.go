package github

import (
	"encoding/json"
	"fmt"
)

type RepoContent struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	SHA         string `json:"sha"`
	Size        int64  `json:"size"`
	URL         string `json:"url"`
	HTMLURL     string `json:"html_url"`
	GitURL      string `json:"git_url"`
	DownloadURL string `json:"download_url"`
	Type        string `json:"type"`
	Links       Links  `json:"_links"`
}

type Links struct {
	Self string `json:"self"`
	Git  string `json:"git"`
	HTML string `json:"html"`
}

func GetContents(gh *GitHubClient, path string) ([]RepoContent, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s/%s/contents/%s", gh.Owner, gh.Repo, path)
	body, err := gh.Get(url, nil)
	if err != nil {
		return nil, err
	}

	return UnmarshalContents(body)
}

func UnmarshalContents(data []byte) ([]RepoContent, error) {
	var r []RepoContent
	err := json.Unmarshal(data, &r)
	return r, err
}
