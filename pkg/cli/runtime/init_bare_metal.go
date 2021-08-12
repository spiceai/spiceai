package runtime

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/github"
	"golang.org/x/mod/semver"
)

func getCurrentVersion() (string, error) {
	spiceCMD := binaryFilePath(config.SpiceBinPath(), "spiced")
	version, err := exec.Command(spiceCMD, "version").Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSuffix(string(version), "\n"), nil
}

func initBareMetal() error {
	binDir := config.SpiceBinPath()
	var isUpgrade bool = false
	var release *github.RepoRelease = nil

	if !isBinaryInstallationRequired(runtimeFileName, binDir) {
		currentVersion, err := getCurrentVersion()
		if err != nil {
			return err
		}

		if currentVersion == "edge" {
			fmt.Println("Using latest 'edge' runtime version.")
			return nil
		}

		release, err := getLatestRuntimeRelease()
		if err != nil {
			return err
		}

		latestRelease := trimSuffix(release.TagName)
		if semver.Compare(currentVersion, latestRelease) == 0 {
			fmt.Println("The Spice runtime is at the latest version.")
			return nil
		}

		isUpgrade = true
	}

	if isUpgrade {
		fmt.Println("New Spice runtime version detected, downloading...")
	} else {
		fmt.Println("The Spice runtime has not yet been installed.")
	}

	err := prepareInstallDir(binDir)
	if err != nil {
		return err
	}

	err = EnsureAIPresent()
	if err != nil {
		return err
	}

	assetName := getAssetName()

	if release == nil {
		release, err = getLatestRuntimeRelease()
		if err != nil {
			return err
		}
	}

	runtimeVersion := trimSuffix(release.TagName)

	fmt.Printf("Downloading and installing Spice Runtime %s binaries...\n", runtimeVersion)

	err = github.DownloadPrivateReleaseAsset(githubClient, *release, assetName, binDir)
	if err != nil {
		fmt.Println("Error downloading Spice runtime binaries.")
		return err
	}

	releaseFilePath := filepath.Join(binDir, runtimeFileName)

	err = makeExecutable(releaseFilePath)
	if err != nil {
		fmt.Println("Error downloading Spice runtime binaries.")
		return err
	}

	fmt.Printf("Spice runtime installed into %s successfully.\n", binDir)

	return nil
}

// Check if the previous version is already installed.
func isBinaryInstallationRequired(binaryFilePrefix, installDir string) bool {
	binaryPath := binaryFilePath(installDir, binaryFilePrefix)

	// first time install?
	_, err := os.Stat(binaryPath)
	return errors.Is(err, os.ErrNotExist)
}

func prepareInstallDir(binDir string) error {
	err := os.MkdirAll(binDir, 0777)
	if err != nil {
		return err
	}

	err = os.Chmod(binDir, 0777)
	if err != nil {
		return err
	}

	return nil
}

func binaryFilePath(binaryDir string, binaryFilePrefix string) string {
	return filepath.Join(binaryDir, binaryFilePrefix)
}

func CreateDirectory(dir string) error {
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return nil
	}
	return os.Mkdir(dir, 0777)
}

func makeExecutable(filepath string) error {
	return os.Chmod(filepath, 0777)
}
