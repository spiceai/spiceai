package github

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/spiceai/spice/pkg/util"
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

func DownloadPrivateReleaseAsset(gh *GitHubClient, release RepoRelease, assetName string, downloadDir string) error {
	if release.Assets == nil || len(release.Assets) == 0 {
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

	assetUrl := fmt.Sprintf("https://%s:@api.github.com/repos/%s/%s/releases/assets/%d", gh.Token, gh.Owner, gh.Repo, asset.ID)

	body, err := gh.call("GET", assetUrl, nil, "application/octet-stream") // Do not pass token, authenticated inline
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
