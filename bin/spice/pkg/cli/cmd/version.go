package cmd

import (
	"fmt"
	"log"
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
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("CLI version:     %s\n", version.Version())

		var rtversion string
		var err error

		rtcontext := context.NewContext()
		err = rtcontext.Init()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if rtcontext.IsRuntimeInstallRequired() {
			rtversion = "not installed"
		} else {
			rtversion, err = rtcontext.Version()
			if err != nil {
				cmd.Printf("error getting runtime version: %s\n", err)
				os.Exit(1)
			}
		}

		cmd.Printf("Runtime version: %s\n", rtversion)

		err = checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
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
			log.Printf("failed to write version file: %s\n", err.Error())
		}
		latestReleaseVersion = release.TagName
	}

	cliVersion := version.Version()
	if cliVersion != "local" && cliVersion != latestReleaseVersion {
		fmt.Printf("\nCLI version %s is now available!\nTo upgrade, run \"spice upgrade\".\n", aurora.BrightGreen(latestReleaseVersion))
	}

	return nil
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
