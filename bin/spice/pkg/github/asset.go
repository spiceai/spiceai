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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type ReleaseAsset struct {
	URL                string `json:"url"`
	BrowserDownloadURL string `json:"browser_download_url"`
	ID                 int64  `json:"id"`
	NodeID             string `json:"node_id"`
	Name               string `json:"name"`
	Label              string `json:"label"`
	State              string `json:"state"`
	ContentType        string `json:"content_type"`
	Size               int64  `json:"size"`
	DownloadCount      int64  `json:"download_count"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
	Uploader           Author `json:"uploader"`
}

func DownloadReleaseAsset(gh *GitHubClient, release *RepoRelease, assetName string, downloadDir string) error {
	if len(release.Assets) == 0 {
		return errors.New("no release assets found")
	}

	var asset *ReleaseAsset
	for _, a := range release.Assets {
		if a.Name == assetName {
			asset = &a
			break
		}
	}

	if asset == nil {
		return errors.New("no matching asset found")
	}

	assetUrl := fmt.Sprintf("https://api.github.com/repos/%s/%s/releases/assets/%d", gh.Owner, gh.Repo, asset.ID)

	body, err := gh.call("GET", assetUrl, nil, "application/octet-stream")
	if err != nil {
		return err
	}

	ext := path.Ext(assetName)
	switch ext {
	case ".zip":
		return util.ExtractZip(body, downloadDir)
	case ".gz":
		return util.ExtractTarGz(body, downloadDir)
	default:
		filePath := filepath.Join(downloadDir, assetName)
		return os.WriteFile(filePath, body, 0766)
	}
}
