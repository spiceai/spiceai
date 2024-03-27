/*
 Copyright 2024 Spice AI, Inc.

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
		cmd.Println("Checking for latest Spice CLI release...")
		release, err := github.GetLatestCliRelease()
		if err != nil {
			cmd.PrintErrln("Error checking for latest release:", err)
			return
		}

		rtcontext := context.NewContext()
		cliVersion := version.Version()

		if cliVersion == release.TagName {
			cmd.Printf("Using the latest version %s. No upgrade required.\n", release.TagName)
			return
		}

		assetName := github.GetAssetName(constants.SpiceCliFilename)
		spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

		cmd.Println("Upgrading the Spice.ai CLI ...")

		stat, err := os.Stat(spiceBinDir)
		if err != nil {
			cmd.PrintErrln("Error upgrading the spice binary:", err)
			return
		}

		tmpDirName := strconv.FormatInt(time.Now().Unix(), 16)
		tmpDir := filepath.Join(spiceBinDir, tmpDirName)

		err = os.Mkdir(tmpDir, stat.Mode())
		if err != nil {
			cmd.PrintErrln("Error upgrading the spice binary:", err)
			return
		}
		defer os.RemoveAll(tmpDir)

		err = github.DownloadAsset(release, tmpDir, assetName)
		if err != nil {
			cmd.PrintErrln("Error downloading the spice binary:", err)
			return
		}

		tempFilePath := filepath.Join(tmpDir, constants.SpiceCliFilename)

		err = util.MakeFileExecutable(tempFilePath)
		if err != nil {
			cmd.PrintErrln("Error upgrading the spice binary:", err)
			return
		}

		releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

		err = os.Rename(tempFilePath, releaseFilePath)
		if err != nil {
			cmd.PrintErrln("Error upgrading the spice binary:", err)
			return
		}

		cmd.Printf("Spice.ai CLI upgraded to %s successfully.\n", release.TagName)
	},
}

func init() {
	RootCmd.AddCommand(upgradeCmd)
}
