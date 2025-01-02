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
