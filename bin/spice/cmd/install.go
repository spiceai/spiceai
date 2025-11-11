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
	"golang.org/x/mod/semver"
)

type cleanupInfo struct {
	tmpDir     string
	markerPath string
	oldBinary  string
}

var installCmd = &cobra.Command{
	Use:     "install [version] [flavor]",
	Aliases: []string{"i"},
	Short:   "Install or reinstall the Spice.ai runtime and CLI",
	Args:    cobra.MaximumNArgs(2), // optional version and flavor arguments
	Example: `
spice install
spice install ai
spice install v1.8.3
spice install v1.8.3 ai

# See more at: https://spiceai.org/docs/
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		// Parse arguments: can be [version], [flavor], or [version, flavor]
		var targetVersion string
		flavor := constants.FlavorDefault
		
		for _, arg := range args {
			// Check if it's a version (starts with 'v' followed by numbers)
			if strings.HasPrefix(arg, "v") && len(arg) > 1 {
				if !semver.IsValid(arg) {
					slog.Error(fmt.Sprintf("Invalid version format: %s. Expected format: v1.8.3", arg))
					os.Exit(1)
				}
				targetVersion = arg
			} else {
				// Otherwise treat as flavor
				var err error
				flavor, err = constants.ParseFlavor(arg)
				if err != nil {
					slog.Error("Invalid argument. Expected version (e.g., v1.8.3) or flavor (e.g., ai)")
					os.Exit(1)
				}
			}
		}

		if targetVersion == "" {
			slog.Info("Checking for latest Spice runtime release...")
		} else {
			slog.Info(fmt.Sprintf("Installing Spice version %s...", targetVersion))
		}

		err = checkLatestCliReleaseVersion(rtcontext)
		if err != nil && util.IsDebug() {
			slog.Error("failed to check for latest CLI release version", "error", err)
		}

		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			slog.Error("getting force flag", "error", err)
			os.Exit(1)
		}

		cpu, err := cmd.Flags().GetBool("cpu")
		if err != nil {
			slog.Error("getting CPU flag", "error", err)
			os.Exit(1)
		}

		if cpu && flavor != constants.FlavorAI {
			slog.Error("CPU flag is only allowed when installing the 'ai' flavor. Try: `spice install ai --cpu`")
			os.Exit(1)
		}

		// Install specific version if provided
		if targetVersion != "" {
			// Install runtime FIRST (while we still have control)
			err = rtcontext.InstallSpecificRuntime(targetVersion, flavor, !cpu)
			if err != nil {
				slog.Error("installing runtime", "error", err)
				os.Exit(1)
			}
			
			// Then install CLI - this may restart the process
			if !installSpecificCli(targetVersion, flavor, !cpu, rtcontext) {
				// CLI install failed
				return
			}

			// If we get here, CLI was already at target version
			slog.Info(fmt.Sprintf("Spice.ai version %s installed successfully", targetVersion))
			return
		}

		// Otherwise install the default (CLI-matching) version
		var installed bool
		if force {
			err = rtcontext.InstallMatchingRuntime(flavor, !cpu)
			if err != nil {
				slog.Error("installing runtime", "error", err)
				os.Exit(1)
			}
			installed = true
		} else {
			installed, err = rtcontext.EnsureInstalled(flavor, true, !cpu)
			if err != nil {
				slog.Error("verifying runtime install", "error", err)
				os.Exit(1)
			}
		}

		if !installed {
			slog.Info("Spice.ai runtime already installed")
		}
	},
}

// installSpecificCli installs a specific version of the CLI
// Returns true if no CLI install was needed (already at version)
// Returns false if install failed or completed (with process restart)
func installSpecificCli(targetVersion string, flavor constants.Flavor, allowAccelerator bool, rtcontext *context.RuntimeContext) bool {
	cliVersion := version.Version()
	
	// Check if we're already at the target version
	if cliVersion == targetVersion {
		slog.Info(fmt.Sprintf("CLI already at version %s", targetVersion))
		return true
	}

	// Check installation path
	spicePathVar, spicePath, err := rtcontext.SpicePath()
	if err != nil {
		slog.Error("finding spice binary location", "error", err)
		return false
	}

	switch spicePathVar {
	case constants.BrewInstall:
		slog.Info("Spice is installed via Homebrew. To install a specific version, reinstall manually.")
		return false
	case constants.OtherInstall:
		slog.Warn(fmt.Sprintf("Spice CLI found at non-standard location '%s'. CLI will not be reinstalled, only runtime.", spicePath))
		return true
	}

	// Get the release
	release, err := github.GetRuntimeRelease(targetVersion)
	if err != nil {
		slog.Error("fetching release", "error", err)
		return false
	}

	assetName := github.GetAssetName(constants.SpiceCliFilename)
	spiceBinDir := filepath.Join(rtcontext.SpiceRuntimeDir(), "bin")

	slog.Info(fmt.Sprintf("Installing Spice.ai CLI %s ...", release.TagName))

	stat, err := os.Stat(spiceBinDir)
	if err != nil {
		slog.Error("accessing spice bin directory", "error", err)
		return false
	}

	tmpDirName := strconv.FormatInt(time.Now().Unix(), 16)
	tmpDir := filepath.Join(spiceBinDir, tmpDirName)

	err = os.Mkdir(tmpDir, stat.Mode())
	if err != nil {
		slog.Error("creating temp directory", "error", err)
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
		slog.Error("making binary executable", "error", err)
		return false
	}

	releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

	// On Windows, move old binary to temp location first
	if util.IsWindows() {
		cleanup := createCleanupInfo()
		if err := os.MkdirAll(cleanup.tmpDir, stat.Mode()); err != nil {
			slog.Error("creating temp directory", "error", err)
			return false
		}
		if err := os.Rename(releaseFilePath, cleanup.oldBinary); err != nil {
			slog.Error("moving old CLI", "error", err)
			return false
		}
		if err := os.WriteFile(cleanup.markerPath, []byte{}, 0644); err != nil {
			slog.Error("creating cleanup marker", "error", err)
			return false
		}
	}

	// Move new cli to the release file path
	err = os.Rename(tempFilePath, releaseFilePath)
	if err != nil {
		slog.Error("installing the spice binary", "error", err)
		return false
	}
	if err := os.RemoveAll(tmpDir); err != nil {
		slog.Error("failed to remove temporary directory", "path", tmpDir, "error", err)
	}

	slog.Info(fmt.Sprintf("Spice.ai CLI installed to %s successfully.", release.TagName))

	// Update the CLI version cache file
	versionFilePath := filepath.Join(rtcontext.SpiceRuntimeDir(), "cli_version.txt")
	if err := os.WriteFile(versionFilePath, []byte(release.TagName+"\n"), 0644); err != nil {
		slog.Warn("failed to update CLI version cache", "error", err)
	}

	// For install command, we don't need to restart - just show success
	slog.Info(fmt.Sprintf("Spice.ai version %s installed successfully. Runtime is ready to use.", release.TagName))
	
	// For unix, the binary has been replaced
	// For windows, we need cleanup on next run
	return false
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

func restartWithNewCli(cliPath string, args []string) error {
	// windows: Prompt the user to restart the CLI
	if runtime.GOOS == "windows" {
		slog.Info("Installation complete. Please restart your terminal to use the new version.")
		return nil
	}

	// unix: Replace the current process with the new cli
	// Use the upgrade reload env variable to signal that we just upgraded
	execEnv := append(os.Environ(), fmt.Sprintf("%s=true", constants.SpiceUpgradeReloadEnv))
	return syscall.Exec(cliPath, args, execEnv)
}

func init() {
	installCmd.Flags().BoolP("force", "f", false, "Force installation of the latest released runtime")
	installCmd.Flags().BoolP("cpu", "c", false, "Install the CPU accelerated version of the AI runtime")
	RootCmd.AddCommand(installCmd)
}
