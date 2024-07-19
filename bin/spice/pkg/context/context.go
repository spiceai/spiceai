/*
Copyright 2024 The Spice.ai OSS Authors

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

package context

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"golang.org/x/mod/semver"
)

type RuntimeContext struct {
	spiceRuntimeDir string
	spiceBinDir     string
	appDir          string
	podsDir         string
	httpEndpoint    string
}

func NewContext() *RuntimeContext {
	rtcontext := &RuntimeContext{
		httpEndpoint: "http://127.0.0.1:8090",
	}
	err := rtcontext.Init()
	if err != nil {
		panic(err)
	}
	return rtcontext
}

func (c *RuntimeContext) SpiceRuntimeDir() string {
	return c.spiceRuntimeDir
}

func (c *RuntimeContext) AppDir() string {
	return c.appDir
}

func (c *RuntimeContext) PodsDir() string {
	return c.podsDir
}

func (c *RuntimeContext) HttpEndpoint() string {
	return c.httpEndpoint
}

func (c *RuntimeContext) Init() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	c.spiceRuntimeDir = filepath.Join(homeDir, constants.DotSpice)
	c.spiceBinDir = filepath.Join(c.spiceRuntimeDir, "bin")

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	c.appDir = cwd
	c.podsDir = filepath.Join(c.appDir, constants.SpicePodsDirectoryName)

	return nil
}

func (c *RuntimeContext) Version() (string, error) {
	spiceCMD := c.binaryFilePath(constants.SpiceRuntimeFilename)
	version, err := exec.Command(spiceCMD, "--version").Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(version)), nil
}

func (c *RuntimeContext) RuntimeUnavailableError() error {
	return fmt.Errorf("The Spice runtime is unavailable at %s. Is it running?", c.httpEndpoint)
}

func (c *RuntimeContext) IsRuntimeInstallRequired() bool {
	binaryPath := c.binaryFilePath(constants.SpiceRuntimeFilename)

	// first time install?
	_, err := os.Stat(binaryPath)
	return errors.Is(err, os.ErrNotExist)
}

func (c *RuntimeContext) InstallOrUpgradeRuntime() error {
	err := c.prepareInstallDir()
	if err != nil {
		return err
	}

	release, err := github.GetLatestRuntimeRelease()
	if err != nil {
		return err
	}

	runtimeVersion := release.TagName

	fmt.Printf("Downloading and installing Spice.ai Runtime %s ...\n", runtimeVersion)

	err = github.DownloadRuntimeAsset(release, c.spiceBinDir)
	if err != nil {
		fmt.Println("Error downloading Spice.ai runtime binaries.")
		return err
	}

	releaseFilePath := filepath.Join(c.spiceBinDir, constants.SpiceRuntimeFilename)

	err = util.MakeFileExecutable(releaseFilePath)
	if err != nil {
		fmt.Println("Error downloading Spice runtime binaries.")
		return err
	}

	fmt.Printf("Spice runtime installed into %s successfully.\n", c.spiceBinDir)

	return nil
}

func (c *RuntimeContext) IsRuntimeUpgradeAvailable() (string, error) {
	currentVersion, err := c.Version()
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(currentVersion, "local") || strings.Contains(currentVersion, "rc") {
		return "", nil
	}

	release, err := github.GetLatestRuntimeRelease()
	if err != nil {
		return "", err
	}

	if semver.Compare(currentVersion, release.TagName) >= 0 {
		return "", nil
	}

	return release.TagName, nil
}

func (c *RuntimeContext) GetSpiceAppRelativePath(absolutePath string) string {
	if strings.HasPrefix(absolutePath, c.appDir) {
		return absolutePath[len(c.appDir)+1:]
	}
	return absolutePath
}

func (c *RuntimeContext) GetRunCmd() (*exec.Cmd, error) {
	spiceCMD := c.binaryFilePath("spiced")

	cmd := exec.Command(spiceCMD, "--metrics", "127.0.0.1:9090")

	return cmd, nil
}

func (c *RuntimeContext) prepareInstallDir() error {
	err := os.MkdirAll(c.spiceBinDir, 0777)
	if err != nil {
		return err
	}

	err = os.Chmod(c.spiceBinDir, 0777)
	if err != nil {
		return err
	}

	return nil
}

func (c *RuntimeContext) binaryFilePath(binaryFilePrefix string) string {
	return filepath.Join(c.spiceBinDir, binaryFilePrefix)
}
