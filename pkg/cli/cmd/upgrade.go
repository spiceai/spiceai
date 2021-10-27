package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/github"
	"github.com/spiceai/spiceai/pkg/util"
)

var upgradeCmd = &cobra.Command{
	Use:   "upgrade RuntTime",
	Short: "upgrades Spice RunTime to the latest release",
	Example: `
spice upgrade
`,
	Run: func(cmd *cobra.Command, args []string) {

		var upgradeVersion string
		var err error
		var shouldInstall bool

		rtcontext := context.CurrentContext()

		upgradeVersion, err = rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		} else if upgradeVersion != "" {
			shouldInstall = true
		}

		if shouldInstall {
			cmd.Printf("Upgrading CLI ...")
			release, err := github.GetLatestCliRelease()
			if err != nil {
				log.Println((err.Error()))
			}

			assetName := github.GetAssetName(constants.SpiceCliFilename)
			spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

			err = github.DownloadAsset(release, spiceBinDir, assetName)
			if err != nil {
				cmd.Println("Error downloading Spice.ai runtime binaries.")
				os.Exit(1)
			}

			releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

			err = util.MakeFileExecutable(releaseFilePath)
			if err != nil {
				fmt.Println("Error downloading Spice runtime binaries.")
				os.Exit(1)
			}
			fmt.Printf("Spice CLI installed into %s successfully.\n", spiceBinDir)
		} else {
			cmd.Println("Already Up To Date with latest CLI")
		}
	},
}

func init() {
	upgradeCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(upgradeCmd)
}
