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
	"time"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

var upgradeCmd = &cobra.Command{
	Use:   "upgrade [version]",
	Short: "Upgrades the Spice CLI and runtime to the latest or specified version",
	Args:  cobra.MaximumNArgs(1),
	Example: `
spice upgrade
spice upgrade v1.8.3
`,
	Run: func(cmd *cobra.Command, args []string) {
		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			slog.Error("getting force flag", "error", err)
			return
		}

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("initializing runtime context", "error", err)
			os.Exit(1)
		}

		// Parse optional version argument
		var targetVersion string
		if len(args) > 0 {
			targetVersion = args[0]
			if !strings.HasPrefix(targetVersion, "v") {
				slog.Error(fmt.Sprintf("Invalid version format: %s. Expected format: v1.8.3", targetVersion))
				os.Exit(1)
			}
		}

		// Special handling for version-specific upgrades: install both runtime and CLI without restart
		if targetVersion != "" {
			slog.Info(fmt.Sprintf("Upgrading to Spice version %s...", targetVersion))

			// Determine flavor from current installation
			flavor := constants.FlavorCore
			models, accelerated := rtcontext.ModelsFlavorInstalled()
			if models {
				flavor = constants.FlavorAI
			}

			// Install runtime first (sets version lock)
			err = rtcontext.InstallSpecificRuntime(targetVersion, flavor, accelerated)
			if err != nil {
				slog.Error("installing runtime", "error", err)
				os.Exit(1)
			}
			slog.Info(fmt.Sprintf("Runtime upgraded to %s successfully.", targetVersion))

			// Then upgrade CLI - note: returns false on success (ready to restart)
			// But we won't restart - we'll just exit with success
			upgradeCli(false, targetVersion, true, rtcontext)

			slog.Info(fmt.Sprintf("Spice upgraded to %s successfully.", targetVersion))
			return
		}

		// For latest version upgrade, use the restart pattern
		if os.Getenv(constants.SpiceUpgradeReloadEnv) != "true" {
			// Run CLI upgrade
			if !upgradeCli(force, "", false, rtcontext) {
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

		// For runtime upgrades, default to the flavor that was installed previously.
		flavor := constants.FlavorCore
		models, accelerated := rtcontext.ModelsFlavorInstalled()
		if models {
			flavor = constants.FlavorAI
		}

		// Upgrade to latest
		runtimeUpgradeRequired, err := rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			slog.Error("checking for runtime upgrade", "error", err)
			return
		}

		if runtimeUpgradeRequired == "" {
			slog.Info(fmt.Sprintf("Using version %s. Runtime upgrade not required.", currentVersion))
			return
		}

		release, err := github.GetRuntimeRelease(version.Version())
		if err != nil {
			slog.Error("installing runtime", "error", err)
			os.Exit(1)
		}

		err = rtcontext.InstallMatchingRuntime(flavor, accelerated) // retain the current accelerator setting for upgrades
		if err != nil {
			slog.Error("installing runtime", "error", err)
			os.Exit(1)
		}

		// Clear version lock when upgrading to latest (no version specified)
		err = rtcontext.ClearVersionLock()
		if err != nil {
			slog.Warn("failed to clear version lock", "error", err)
		}

		slog.Info(fmt.Sprintf("Spice runtime upgraded to %s successfully.", release.TagName))
	},
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
func upgradeCli(force bool, targetVersion string, skipRestart bool, rtcontext *context.RuntimeContext) bool {
	var release *github.RepoRelease
	var err error

	if targetVersion != "" {
		slog.Info(fmt.Sprintf("Checking for Spice CLI release %s...", targetVersion))
		release, err = github.GetRuntimeRelease(targetVersion)
		if err != nil {
			slog.Error("checking for release", "error", err)
			return false
		}
	} else {
		slog.Info("Checking for latest Spice CLI release...")
		release, err = github.GetLatestCliRelease()
		if err != nil {
			slog.Error("checking for latest release", "error", err)
			return false
		}
	}

	cliVersion := version.Version()
	if cliVersion == release.TagName && !force {
		slog.Info(fmt.Sprintf("Using the latest version %s. CLI upgrade not required.", release.TagName))
		return true
	}

	spicePathVar, spicePath, err := rtcontext.SpicePath()
	if err != nil {
		slog.Error("finding spice binary location", "error", err)
		os.Exit(1)
	}

	switch spicePathVar {
	case constants.BrewInstall:
		slog.Info("Spice is installed via Homebrew. Upgrade the CLI and Runtime by running:\n\n  brew upgrade spiceai/spiceai/spice\n")
		return false
	case constants.OtherInstall:
		msg := fmt.Sprintf("Spice upgrade failed: The Spice CLI is installed in a non-standard location: '%s'.\n\n"+
			"To upgrade:\n"+
			"1. Remove the existing installation. Example:\n"+
			"   rm -rf %s\n\n"+
			"2. Reinstall Spice by following the instructions at:\n"+
			"   https://spiceai.org/docs/installation", spicePath, spicePath)
		slog.Info(msg)
		return false
	}

	assetName := github.GetAssetName(constants.SpiceCliFilename)
	spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

	slog.Info(fmt.Sprintf("Found version %s, upgrading the Spice.ai CLI ...", release.TagName))

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
	if err := os.RemoveAll(tmpDir); err != nil {
		slog.Error("failed to remove temporary directory", "path", tmpDir, "error", err)
	}

	slog.Info(fmt.Sprintf("Spice.ai CLI upgraded to %s successfully.", release.TagName))

	// Skip restart if requested (for version-specific upgrades)
	if skipRestart {
		return true
	}

	execArgs := []string{releaseFilePath}
	execArgs = append(execArgs, os.Args[1:]...)
	if err := restartWithNewCli(releaseFilePath, execArgs); err != nil {
		slog.Error("restarting CLI", "error", err)
	}

	// For unix, this is unreachable
	// For windows, the CLI will be restarted with the new binary, return false to terminate old CLI
	return false
}

func init() {
	upgradeCmd.Flags().BoolP("force", "f", false, "Force upgrade to the latest released version")
	RootCmd.AddCommand(upgradeCmd)
}
