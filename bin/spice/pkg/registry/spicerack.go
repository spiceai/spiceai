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

package registry

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	spice_http "github.com/spiceai/spiceai/bin/spice/pkg/http"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type SpiceRackRegistry struct{}

func getSpiceRackBaseUrl() string {
	if strings.HasSuffix(version.Version(), "-dev") {
		return "https://dev-data.spiceai.io/v1"
	} else {
		return "https://api.spicerack.org/v1"
	}
}

func (r *SpiceRackRegistry) GetPod(ctx *context.RuntimeContext, podFullPath string) (string, error) {
	parts := strings.Split(podFullPath, "@")
	podPath := podFullPath
	podVersion := ""
	if len(parts) == 2 {
		podPath = parts[0]
		podVersion = parts[1]
	}

	url := fmt.Sprintf("%s/spicepods/%s", getSpiceRackBaseUrl(), podPath)
	if podVersion != "" {
		url = fmt.Sprintf("%s/%s", url, podVersion)
	}
	failureMessage := fmt.Sprintf("An error occurred while fetching Spicepod '%s' from spicerack.org", podFullPath)

	response, err := spice_http.Get(url, "application/zip", ctx.GetHeaders())
	if err != nil {
		slog.Debug(fmt.Sprintf("%s: %s", failureMessage, err.Error()))
		return "", errors.New(failureMessage)
	}
	defer response.Body.Close()

	if response.StatusCode == 404 {
		return "", NewRegistryItemNotFound(fmt.Errorf("spicepod %s not found", podPath))
	}

	if response.StatusCode != 200 {
		return "", fmt.Errorf("an error occurred fetching Spicepod '%s'", podPath)
	}

	tmpFile, err := os.CreateTemp(os.TempDir(), "spice-")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, response.Body)
	if err != nil {
		return "", err
	}

	podsPath := ctx.PodsDir()
	podsPathWithName := filepath.Join(podsPath, podPath)

	podsPerm, err := util.MkDirAllInheritPerm(podsPathWithName)
	if err != nil {
		return "", err
	}

	zipReader, err := zip.OpenReader(tmpFile.Name())
	if err != nil {
		return "", err
	}

	for _, f := range zipReader.File {
		fpath := filepath.Join(podsPathWithName, f.Name)

		extractDir := filepath.Dir(fpath)
		err = util.SanitizeExtractPath(fpath, extractDir)
		if err != nil {
			return "", err
		}

		if f.FileInfo().IsDir() {
			err := os.MkdirAll(fpath, podsPerm)
			if err != nil {
				return "", err
			}
			continue
		}

		err = os.MkdirAll(extractDir, podsPerm)
		if err != nil {
			return "", err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return "", err
		}
		defer outFile.Close()

		zipFile, err := f.Open()
		if err != nil {
			return "", err
		}
		defer zipFile.Close()

		_, err = io.Copy(outFile, zipFile)
		if err != nil {
			return "", err
		}
	}

	return podsPathWithName, nil
}
