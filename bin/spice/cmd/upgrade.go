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
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

var upgradeCmd = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrades the Spice CLI to the latest release",
	Example: `
spice upgrade
`,
	Run: func(cmd *cobra.Command, args []string) {
		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			slog.Error("getting force flag", "error", err)
			return
		}

		slog.Info("Checking for latest Spice CLI release...")
		release, err := github.GetLatestCliRelease()
		if err != nil {
			slog.Error("checking for latest release", "error", err)
			return
		}

		rtcontext := context.NewContext()
		err = rtcontext.Init()
		if err != nil {
			slog.Error("initializing runtime context", "error", err)
			os.Exit(1)
		}
		cliVersion := version.Version()

		runtimeUpgradeRequired, err := rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			slog.Error("checking for runtime upgrade", "error", err)
			return
		}

		if cliVersion == release.TagName && runtimeUpgradeRequired != "" && !force {
			slog.Info(fmt.Sprintf("Using the latest version %s. No upgrade required.", release.TagName))
			return
		}

		assetName := github.GetAssetName(constants.SpiceCliFilename)
		spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

		slog.Info("Upgrading the Spice.ai CLI ...")

		stat, err := os.Stat(spiceBinDir)
		if err != nil {
			slog.Error("upgrading the spice binary", "error", err)
			return
		}

		tmpDirName := strconv.FormatInt(time.Now().Unix(), 16)
		tmpDir := filepath.Join(spiceBinDir, tmpDirName)

		err = os.Mkdir(tmpDir, stat.Mode())
		if err != nil {
			slog.Error("upgrading the spice binary", "error", err)
			return
		}
		defer os.RemoveAll(tmpDir)

		err = github.DownloadAsset(release, tmpDir, assetName)
		if err != nil {
			slog.Error("downloading the spice binary", "error", err)
			return
		}

		tempFilePath := filepath.Join(tmpDir, constants.SpiceCliFilename)

		err = util.MakeFileExecutable(tempFilePath)
		if err != nil {
			slog.Error("upgrading the spice binary", "error", err)
			return
		}

		releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

		// On Windows, it is not possible to overwrite a binary while it's running.
		// However, it can be moved/renamed making it possible to save new release with the original name.
		if util.IsWindows() {
			runningCliTempLocation := filepath.Join(spiceBinDir, constants.SpiceCliFilename+".bak")
			err = os.Rename(releaseFilePath, runningCliTempLocation)
			if err != nil {
				slog.Error("upgrading the spice binary", "error", err)
				return
			}
		}

		err = os.Rename(tempFilePath, releaseFilePath)
		if err != nil {
			slog.Error("upgrading the spice binary", "error", err)
			return
		}

		slog.Info(fmt.Sprintf("Spice.ai CLI upgraded to %s successfully.", release.TagName))

		flavor := ""
		if rtcontext.ModelsFlavorInstalled() {
			flavor = "ai"
		}

		err = rtcontext.InstallOrUpgradeRuntime(flavor)
		if err != nil {
			slog.Error("installing runtime", "error", err)
			os.Exit(1)
		}

		slog.Info(fmt.Sprintf("Spice runtime upgraded to %s successfully.", release.TagName))
	},
}

func init() {
	upgradeCmd.Flags().BoolP("force", "f", false, "Force upgrade to the latest released version")
	RootCmd.AddCommand(upgradeCmd)
}
