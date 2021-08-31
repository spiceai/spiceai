package metal

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/github"
	"github.com/spiceai/spiceai/pkg/util"
	spice_version "github.com/spiceai/spiceai/pkg/version"
)

type MetalContext struct {
	spiceRuntimeDir       string
	spiceBinDir           string
	aiEngineDir           string
	aiEnginePythonCmdPath string
	appDir                string
	podsDir               string
}

func NewMetalContext() *MetalContext {
	homeDir := os.Getenv("HOME")

	spiceRuntimeDir := filepath.Join(homeDir, constants.DotSpice)
	spiceBinDir := filepath.Join(spiceRuntimeDir, "bin")
	aiEngineDir := filepath.Join(spiceBinDir, "ai")
	aiEnginePythonCmdPath := filepath.Join(aiEngineDir, "venv", "bin", constants.PythonCmd)

	return &MetalContext{
		spiceRuntimeDir:       spiceRuntimeDir,
		spiceBinDir:           spiceBinDir,
		aiEngineDir:           aiEngineDir,
		aiEnginePythonCmdPath: aiEnginePythonCmdPath,
	}
}

func (c *MetalContext) SpiceRuntimeDir() string {
	return c.spiceRuntimeDir
}

func (c *MetalContext) AIEngineDir() string {
	return c.aiEngineDir
}

func (c *MetalContext) AIEnginePythonCmdPath() string {
	return c.aiEnginePythonCmdPath
}

func (c *MetalContext) AppDir() string {
	return c.appDir
}

func (c *MetalContext) PodsDir() string {
	return c.podsDir
}

func (c *MetalContext) Init() error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	c.appDir = cwd
	c.podsDir = filepath.Join(c.appDir, constants.DotSpice, "pods")

	return nil
}

func (c *MetalContext) Version() (string, error) {
	spiceCMD := c.binaryFilePath(constants.SpiceRuntimeFilename)
	version, err := exec.Command(spiceCMD, "version").Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(version)), nil
}

func (c *MetalContext) IsRuntimeInstallRequired() bool {
	binaryPath := c.binaryFilePath(constants.SpiceRuntimeFilename)

	// first time install?
	_, err := os.Stat(binaryPath)
	return errors.Is(err, os.ErrNotExist)
}

func (c *MetalContext) InstallOrUpgradeRuntime() error {
	err := c.prepareInstallDir()
	if err != nil {
		return err
	}

	err = c.ensureAIPresent()
	if err != nil {
		return err
	}

	release, err := github.GetLatestRuntimeRelease(spice_version.Version())
	if err != nil {
		return err
	}

	runtimeVersion := github.GetRuntimeVersion(release)

	fmt.Printf("Downloading and installing Spice.ai Runtime %s ...\n", runtimeVersion)

	err = github.DownloadRuntimeAsset(release, c.spiceBinDir)
	if err != nil {
		fmt.Println("Error downloading Spice.ai runtime binaries.")
		return err
	}

	releaseFilePath := filepath.Join(c.spiceBinDir, constants.SpiceRuntimeFilename)

	err = util.MakeFileExecutable(releaseFilePath)
	if err != nil {
		fmt.Println("Error downloading Spice runtime binaries.")
		return err
	}

	fmt.Printf("Spice runtime installed into %s successfully.\n", c.spiceBinDir)

	return nil
}

func (c *MetalContext) IsRuntimeUpgradeAvailable() (string, error) {
	currentVersion, err := c.Version()
	if err != nil {
		return "", err
	}

	if currentVersion == "local" {
		fmt.Println("Using latest 'local' runtime version.")
		return "", nil
	}

	tagName := "v" + currentVersion
	release, err := github.GetLatestRuntimeRelease(tagName)
	if err != nil {
		return "", err
	}

	return release.TagName, nil
}

func (c *MetalContext) GetSpiceAppRelativePath(absolutePath string) string {
	if strings.HasPrefix(absolutePath, c.appDir) {
		return absolutePath[len(c.appDir)+1:]
	}
	return absolutePath
}

func (c *MetalContext) GetRunCmd(manifestPath string) (*exec.Cmd, error) {
	err := c.ensureAIPresent()
	if err != nil {
		return nil, fmt.Errorf("AI Engine has not been downloaded")
	}
	spiceCMD := c.binaryFilePath("spiced")

	cmd := exec.Command(spiceCMD, manifestPath)

	return cmd, nil
}

func (c *MetalContext) prepareInstallDir() error {
	err := os.MkdirAll(c.spiceBinDir, 0777)
	if err != nil {
		return err
	}

	err = os.Chmod(c.spiceBinDir, 0777)
	if err != nil {
		return err
	}

	return nil
}

func (c *MetalContext) binaryFilePath(binaryFilePrefix string) string {
	return filepath.Join(c.spiceBinDir, binaryFilePrefix)
}

func (c *MetalContext) ensureAIPresent() error {
	if _, err := os.Stat(c.aiEngineDir); !os.IsNotExist(err) {
		if err != nil {
			return err
		}
		return nil
	}

	spiceRepoRoot := os.Getenv("SPICE_REPO_ROOT")
	if spiceRepoRoot == "" {
		return errors.New("SPICE_REPO_ROOT is unset. Please ensure it contains the full path to the root of your spiceai repo")
	}

	targetPath := path.Join(spiceRepoRoot, "ai", "src")

	err := os.Symlink(targetPath, c.aiEngineDir)
	if err != nil {
		return err
	}

	return nil
}
