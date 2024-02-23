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
