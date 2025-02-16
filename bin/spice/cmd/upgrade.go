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
	"runtime"
	"strconv"
	"strings"
	"syscall"
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

		rtcontext := context.NewContext()
		err = rtcontext.Init()
		if err != nil {
			slog.Error("initializing runtime context", "error", err)
			os.Exit(1)
		}

		spicePathVar, spicePath, err := rtcontext.SpicePath()
		if err != nil {
			slog.Error("finding spice binary location", "error", err)
			os.Exit(1)
		}

		switch spicePathVar {
		case constants.BrewInstall:
			slog.Info("Spice is downloaded from Homebrew. Run `brew upgrade spiceai/spiceai/spice` to upgrade the CLI.")
			return
		case constants.OtherInstall:
			msg := fmt.Sprintf("Spice upgrade failed: Spice CLI found at non-standard location '%s'. To upgrade:\n"+
				"1. Remove current installation\n"+
				"2. Refer to https://spiceai.org/docs/installation to reinstall spice\n"+
				"3. Try upgrade again", spicePath)
			slog.Info(msg)
			return
		}

		if os.Getenv(constants.SpiceUpgradeReloadEnv) != "true" {
			// Run CLI upgrade
			if !upgradeCli(force, rtcontext) {
				// Exit if CLI upgrade fail / completes
				return
			}
		}

		// Cleanup old binaries on windows
		if runtime.GOOS == "windows" {
			cleanupOldBinaries()
		}

		slog.Info("Checking for the latest Spice Runtime release...")
		currentVersion, err := rtcontext.Version()
		if err != nil {
			slog.Info("Spice runtime is not installed and won't be upgraded. Run `spice install` to install the runtime.")
			return
		}

		runtimeUpgradeRequired, err := rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			slog.Error("checking for runtime upgrade", "error", err)
			return
		}

		if runtimeUpgradeRequired == "" {
			slog.Info(fmt.Sprintf("Using version %s. No upgrade required.", currentVersion))
			return
		}

		// For runtime upgrades, default to the flavor that was installed previously.
		flavor := constants.FlavorCore
		models, accelerated := rtcontext.ModelsFlavorInstalled()
		if models {
			flavor = constants.FlavorAI
		}

		release, err := github.GetLatestRuntimeRelease()
		if err != nil {
			slog.Error("installing runtime", "error", err)
			os.Exit(1)
		}

		err = rtcontext.InstallOrUpgradeRuntime(flavor, accelerated) // retain the current accelerator setting for upgrades
		if err != nil {
			slog.Error("installing runtime", "error", err)
			os.Exit(1)
		}

		slog.Info(fmt.Sprintf("Spice runtime upgraded to %s successfully.", release.TagName))
	},
}

type cleanupInfo struct {
	tmpDir     string
	markerPath string
	oldBinary  string
}

func createCleanupInfo() *cleanupInfo {
	if !util.IsWindows() {
		return nil
	}
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("spice-%d", time.Now().UnixNano()))
	return &cleanupInfo{
		tmpDir:     tmpDir,
		markerPath: filepath.Join(tmpDir, constants.SpiceCliCleanupMarkerFile),
		oldBinary:  filepath.Join(tmpDir, constants.SpiceCliFilename),
	}
}

func cleanupOldBinaries() {
	if !util.IsWindows() {
		return
	}

	// Cleanup old binaries
	entries, err := os.ReadDir(os.TempDir())
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "spice-") {
			tmpDir := filepath.Join(os.TempDir(), entry.Name())
			markerPath := filepath.Join(tmpDir, constants.SpiceCliCleanupMarkerFile)
			if _, err := os.Stat(markerPath); err == nil {
				_ = os.RemoveAll(tmpDir)
			}
		}
	}
}

// Upgrade CLI
// Returns true if the CLI no upgrade was required
// Returns false if the upgrade failed or the CLI upgrade completes
func upgradeCli(force bool, rtcontext *context.RuntimeContext) bool {
	slog.Info("Checking for latest Spice CLI release...")
	release, err := github.GetLatestCliRelease()
	if err != nil {
		slog.Error("checking for latest release", "error", err)
		return false
	}

	cliVersion := version.Version()
	if cliVersion == release.TagName && !force {
		slog.Info(fmt.Sprintf("Using the latest version %s. No upgrade required.", release.TagName))
		return true
	}

	assetName := github.GetAssetName(constants.SpiceCliFilename)
	spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

	slog.Info("Upgrading the Spice.ai CLI ...")

	stat, err := os.Stat(spiceBinDir)
	if err != nil {
		slog.Error("upgrading the spice binary", "error", err)
		return false
	}

	tmpDirName := strconv.FormatInt(time.Now().Unix(), 16)
	tmpDir := filepath.Join(spiceBinDir, tmpDirName)

	err = os.Mkdir(tmpDir, stat.Mode())
	if err != nil {
		slog.Error("upgrading the spice binary", "error", err)
		return false
	}

	err = github.DownloadAsset(release, tmpDir, assetName)
	if err != nil {
		slog.Error("downloading the spice binary", "error", err)
		return false
	}

	tempFilePath := filepath.Join(tmpDir, constants.SpiceCliFilename)

	err = util.MakeFileExecutable(tempFilePath)
	if err != nil {
		slog.Error("upgrading the spice binary", "error", err)
		return false
	}

	releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

	// On Windows, it is not possible to overwrite a binary while it's running.
	// However, it can be moved/renamed making it possible to save new release with the original name.
	if util.IsWindows() {
		// Create a temp directory under Windows temp folder
		cleanup := createCleanupInfo()
		if err := os.MkdirAll(cleanup.tmpDir, stat.Mode()); err != nil {
			slog.Error("creating temp directory", "error", err)
			return false
		}
		// Move the old binary to the temp directory
		if err := os.Rename(releaseFilePath, cleanup.oldBinary); err != nil {
			slog.Error("moving old CLI", "error", err)
			return false
		}
		// Create a marker file to indicate that the old binary is moved
		if err := os.WriteFile(cleanup.markerPath, []byte{}, 0644); err != nil {
			slog.Error("creating cleanup marker", "error", err)
			return false
		}
	}

	// Move new cli to the release file path, and remove the temp downloading directory
	err = os.Rename(tempFilePath, releaseFilePath)
	if err != nil {
		slog.Error("upgrading the spice binary", "error", err)
		return false
	}
	os.RemoveAll(tmpDir)

	slog.Info(fmt.Sprintf("Spice.ai CLI upgraded to %s successfully.", release.TagName))

	execArgs := []string{releaseFilePath}
	execArgs = append(execArgs, os.Args[1:]...)
	if err := restartWithNewCli(releaseFilePath, execArgs); err != nil {
		slog.Error("restarting CLI", "error", err)
	}

	// For unix, this is unreachable
	// For windows, the CLI will be restarted with the new binary, return false to terminate old CLI
	return false
}

func restartWithNewCli(cliPath string, args []string) error {
	// windows: Prompt the user to restart the CLI
	if runtime.GOOS == "windows" {
		slog.Info("Please rerun the `spice upgrade` command to finish the runtime upgrade.")
		return nil
	}

	// unix: Replace the current process with the new cli
	execEnv := append(os.Environ(), fmt.Sprintf("%s=true", constants.SpiceUpgradeReloadEnv))
	return syscall.Exec(cliPath, args, execEnv)
}

func init() {
	upgradeCmd.Flags().BoolP("force", "f", false, "Force upgrade to the latest released version")
	RootCmd.AddCommand(upgradeCmd)
}
