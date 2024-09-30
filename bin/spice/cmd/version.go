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

package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
	"golang.org/x/mod/semver"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		slog.Info(fmt.Sprintf("CLI version:     %s\n", version.Version()))

		var rtversion string
		var err error

		rtcontext := context.NewContext()
		err = rtcontext.Init()
		if err != nil {
			slog.Error("initializing runtime context", "error", err)
			os.Exit(1)
		}

		if rtcontext.IsRuntimeInstallRequired() {
			rtversion = "not installed"
		} else {
			rtversion, err = rtcontext.Version()
			if err != nil {
				slog.Error(fmt.Sprintf("error getting runtime version: %s\n", err))
				os.Exit(1)
			}
		}

		slog.Info(fmt.Sprintf("Runtime version: %s\n", rtversion))

		err = checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			slog.Error(fmt.Sprintf("failed to check for latest CLI release version: %s\n", err.Error()))
		}
	},
}

func checkLatestCliReleaseVersion() error {
	rtcontext := context.NewContext()

	err := rtcontext.Init()
	if err != nil {
		return err
	}

	var latestReleaseVersion string
	versionFilePath := filepath.Join(rtcontext.SpiceRuntimeDir(), "cli_version.txt")
	if stat, err := os.Stat(versionFilePath); !os.IsNotExist(err) {
		if time.Since(stat.ModTime()) < 24*time.Hour {
			versionData, err := os.ReadFile(versionFilePath)
			if err == nil {
				latestReleaseVersion = strings.TrimSpace(string(versionData))
			}
		}
	}

	if latestReleaseVersion == "" {
		release, err := github.GetLatestCliRelease()
		if err != nil {
			return err
		}
		err = os.WriteFile(versionFilePath, []byte(release.TagName+"\n"), 0644)
		if err != nil && util.IsDebug() {
			slog.Error(fmt.Sprintf("failed to write version file: %s\n", err.Error()))
		}
		latestReleaseVersion = release.TagName
	}

	cliVersion := version.Version()

	cliIsPreRelease := strings.HasPrefix(cliVersion, "local") || strings.Contains(cliVersion, "rc")

	if !cliIsPreRelease && semver.Compare(cliVersion, latestReleaseVersion) < 0 {
		slog.Info(fmt.Sprintf("\nCLI version %s is now available!\nTo upgrade, run \"spice upgrade\".\n", aurora.BrightGreen(latestReleaseVersion)))
	}

	return nil
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
