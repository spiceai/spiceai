package cmd

import (
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/github"
	"github.com/spiceai/spiceai/pkg/util"
	"github.com/spiceai/spiceai/pkg/version"
)

var upgradeCmd = &cobra.Command{
	Use:   "upgrade",
	Short: "Upgrades the Spice CLI to the latest release",
	Example: `
spice upgrade
`,
	Run: func(cmd *cobra.Command, args []string) {
		release, err := github.GetLatestCliRelease()
		if err != nil {
			cmd.PrintErrln("Error checking for latest release: %w", err)
			return
		}

		rtcontext := context.CurrentContext()
		cliVersion := version.Version()

		if cliVersion == release.TagName {
			cmd.Printf("Using the latest version %s. No upgrade required.\n", release.TagName)
			return
		}

		assetName := github.GetAssetName(constants.SpiceCliFilename)
		spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

		cmd.Println("Upgrading the Spice.ai CLI ...")

		err = github.DownloadAsset(release, spiceBinDir, assetName)
		if err != nil {
			cmd.PrintErrln("Error downloading the spice binary: %w", err)
			return
		}

		releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

		err = util.MakeFileExecutable(releaseFilePath)
		if err != nil {
			cmd.PrintErrln("Error downloading the spice binary: %w", err)
			return
		}

		cmd.Printf("Spice.ai CLI upgraded to %s successfully.\n", release.TagName)
	},
}

func init() {
	upgradeCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(upgradeCmd)
}
