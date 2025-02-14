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
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
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

		// Intentionally without structured logging
		cmd.Printf("CLI version:     %s\n", version.Version())

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

		// Intentionally without structured logging
		cmd.Printf("Runtime version: %s\n", rtversion)

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

	cliIsPreRelease := strings.HasPrefix(cliVersion, "local") || strings.Contains(cliVersion, "build")

	if !cliIsPreRelease && semver.Compare(cliVersion, latestReleaseVersion) < 0 {
		spicePathVar, spicePath, err := rtcontext.SpicePath()
		if err != nil {
			return err
		}
		switch spicePathVar {
		case constants.StandardInstall:
			slog.Info(fmt.Sprintf("\nCLI version %s is now available!\nTo upgrade, run \"spice upgrade\".\n", aurora.BrightGreen(latestReleaseVersion)))
		case constants.BrewInstall:
			slog.Info(fmt.Sprintf("\nCLI version %s is now available!\nTo upgrade, run \"brew upgrade spiceai/spiceai/spice\".\n", aurora.BrightGreen(latestReleaseVersion)))
		case constants.OtherInstall:
			msg := fmt.Sprintf("\nCLI version %s is now available!\n"+
				"Spice CLI found at non-standard location '%s'. To upgrade:\n"+
				"1. Remove current installation\n"+
				"2. Refer to https://spiceai.org/docs/installation to reinstall spice\n"+
				"3. Try upgrade again", aurora.BrightGreen(latestReleaseVersion), spicePath)
			slog.Info(msg)
		}
	}

	return nil
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
