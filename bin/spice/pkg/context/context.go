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

package context

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"golang.org/x/mod/semver"
)

const (
	GET  = "GET"
	POST = "POST"
)

type RuntimeContext struct {
	spiceRuntimeDir string
	spiceBinDir     string
	appDir          string
	podsDir         string
	httpEndpoint    string
	metricsEndpoint string
	isCloud         bool
	httpClient      *http.Client
	apiKey          string
	userAgent       string
	extraHeaders    map[string]string
}

func NewContext() *RuntimeContext {
	rtcontext := &RuntimeContext{
		httpEndpoint:    "http://127.0.0.1:8090",
		metricsEndpoint: "http://127.0.0.1:9090",
		httpClient:      &http.Client{},
		userAgent:       util.GetSpiceUserAgent("spice"),
		extraHeaders:    make(map[string]string),
	}
	err := rtcontext.Init()
	if err != nil {
		panic(err)
	}
	return rtcontext
}

func NewHttpsContext(rootCertPath string) *RuntimeContext {
	rootCert, err := os.ReadFile(rootCertPath)
	if err != nil {
		panic(err)
	}

	roots := x509.NewCertPool()
	if ok := roots.AppendCertsFromPEM(rootCert); !ok {
		panic("Failed to append root certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs: roots,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	client := &http.Client{
		Transport: transport,
	}

	rtcontext := &RuntimeContext{
		httpEndpoint:    "https://127.0.0.1:8090",
		metricsEndpoint: "https://127.0.0.1:9090",
		httpClient:      client,
	}

	err = rtcontext.Init()
	if err != nil {
		panic(err)
	}
	return rtcontext
}

func (c *RuntimeContext) Client() *http.Client {
	return c.httpClient
}

func (c *RuntimeContext) SpiceRuntimeDir() string {
	return c.spiceRuntimeDir
}

func (c *RuntimeContext) AppDir() string {
	return c.appDir
}

func (c *RuntimeContext) PodsDir() string {
	return c.podsDir
}

func (c *RuntimeContext) HttpEndpoint() string {
	return c.httpEndpoint
}

func (c *RuntimeContext) MetricsEndpoint() string {
	return c.metricsEndpoint
}

func (c *RuntimeContext) Init() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	c.spiceRuntimeDir = filepath.Join(homeDir, constants.DotSpice)
	c.spiceBinDir = filepath.Join(c.spiceRuntimeDir, "bin")

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	c.appDir = cwd
	c.podsDir = filepath.Join(c.appDir, constants.SpicePodsDirectoryName)

	dotEnvValues, err := loadDotEnvValues()
	if err != nil {
		return err
	}

	if apiKey, ok := dotEnvValues["SPICE_SPICEAI_API_KEY"]; ok {
		c.apiKey = apiKey
	}

	return nil
}

func (c *RuntimeContext) Version() (string, error) {
	spiceCMD := c.binaryFilePath(constants.SpiceRuntimeFilename)
	version, err := exec.Command(spiceCMD, "--version").Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(version)), nil
}

func (c *RuntimeContext) RequireModelsFlavor(cmd *cobra.Command) {
	if models, _ := c.ModelsFlavorInstalled(); models {
		return
	}
	slog.Info("This feature requires a runtime version with AI capabilities enabled. Install (y/n)? ")
	var confirm string
	_, _ = fmt.Scanf("%s", &confirm)
	if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
		slog.Warn("AI-enabled runtime not installed, exiting...")
		os.Exit(0)
	}
	slog.Info("Installing AI-enabled runtime...")
	err := c.InstallOrUpgradeRuntime(constants.FlavorAI, true) // default to using an accelerator for prompted installs
	if err != nil {
		slog.Error("installing models runtime", "error", err)
		os.Exit(1)
	}
}

// Return type = (models, accelerated)
func (c *RuntimeContext) ModelsFlavorInstalled() (models bool, accelerated bool) {
	version, err := c.Version()
	if err != nil {
		return false, false
	}

	// Split the semver string by '+', the part after '+' is the build metadata
	parts := strings.Split(version, "+")
	if len(parts) < 2 {
		// No build metadata present
		return false, false
	}

	// Split build metadata by '.'
	buildMetadata := parts[1]
	metadataParts := strings.Split(buildMetadata, ".")

	models = false
	accelerated = false
	// Check if any of the metadata parts is 'models'
	for _, part := range metadataParts {
		if part == "models" {
			models = true
		}

		if part == "cuda" || part == "metal" {
			accelerated = true
		}
	}

	return
}

func (c *RuntimeContext) RuntimeUnavailableError() error {
	return fmt.Errorf("the Spice runtime is unavailable at %s. Is it running?", c.httpEndpoint)
}

func (c *RuntimeContext) IsRuntimeInstallRequired() bool {
	binaryPath := c.binaryFilePath(constants.SpiceRuntimeFilename)

	// first time install?
	_, err := os.Stat(binaryPath)
	return errors.Is(err, os.ErrNotExist)
}

func (c *RuntimeContext) InstallOrUpgradeRuntime(flavor constants.Flavor, allowAccelerator bool) error {
	err := c.prepareInstallDir()
	if err != nil {
		return err
	}

	release, err := github.GetLatestRuntimeRelease()
	if err != nil {
		return err
	}

	runtimeVersion := release.TagName

	slog.Info(fmt.Sprintf("Downloading and installing Spice.ai Runtime %s ...\n", runtimeVersion))

	err = github.DownloadRuntimeAsset(flavor, release, c.spiceBinDir, allowAccelerator)
	if err != nil {
		slog.Error("downloading Spice.ai runtime binaries", "error", err)
		return err
	}

	releaseFilePath := filepath.Join(c.spiceBinDir, constants.SpiceRuntimeFilename)

	err = util.MakeFileExecutable(releaseFilePath)
	if err != nil {
		slog.Error("downloading Spice runtime binaries.", "error", err)
		return err
	}

	slog.Info(fmt.Sprintf("Spice runtime installed into %s successfully.\n", c.spiceBinDir))

	return nil
}

func (c *RuntimeContext) IsRuntimeUpgradeAvailable() (string, error) {
	currentVersion, err := c.Version()
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(currentVersion, "local") || strings.Contains(currentVersion, "rc") {
		return "", nil
	}

	release, err := github.GetLatestRuntimeRelease()
	if err != nil {
		return "", err
	}

	if semver.Compare(currentVersion, release.TagName) >= 0 {
		return "", nil
	}

	return release.TagName, nil
}

func (c *RuntimeContext) GetSpiceAppRelativePath(absolutePath string) string {
	if strings.HasPrefix(absolutePath, c.appDir) {
		return absolutePath[len(c.appDir)+1:]
	}
	return absolutePath
}

func (c *RuntimeContext) GetRunCmd(args []string) (*exec.Cmd, error) {
	spiceCMD := c.binaryFilePath("spiced")

	spiceArgs := []string{
		"--metrics", "127.0.0.1:9090",
		"--pods-watcher-enabled",
	}
	args = append(spiceArgs, args...)

	cmd := exec.Command(spiceCMD, args...)

	return cmd, nil
}

func (c *RuntimeContext) prepareInstallDir() error {
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

func (c *RuntimeContext) binaryFilePath(binaryFilePrefix string) string {
	return filepath.Join(c.spiceBinDir, binaryFilePrefix)
}

func (c *RuntimeContext) WithCloud(isCloud bool) *RuntimeContext {
	if isCloud {
		c.httpEndpoint = "https://data.spiceai.io"
	} else {
		c.httpEndpoint = "http://localhost:8090"
	}
	c.isCloud = isCloud
	return c
}

func (c *RuntimeContext) SetApiKey(apiKey string) {
	c.apiKey = apiKey
}

func (c *RuntimeContext) GetApiKey() string {
	return c.apiKey
}

func (c *RuntimeContext) SetUserAgent(userAgent string) {
	c.userAgent = userAgent
}

func (c *RuntimeContext) GetUserAgent() string {
	return c.userAgent
}

func (c *RuntimeContext) SetUserAgentClient(client string) {
	c.userAgent = util.GetSpiceUserAgent(client)
}

func (c *RuntimeContext) AddHeaders(headers map[string]string) {
	for key, value := range headers {
		c.extraHeaders[key] = value
	}
}

func (c *RuntimeContext) GetHeaders() map[string]string {
	headers := make(map[string]string)

	if c.isCloud {
		apiKey := os.Getenv("SPICE_API_KEY")
		if apiKey != "" {
			headers["X-API-Key"] = apiKey
		}
	}

	if c.apiKey != "" {
		headers["X-API-Key"] = c.apiKey
	}

	for key, value := range c.extraHeaders {
		headers[key] = value
	}

	return headers
}

func (c *RuntimeContext) IsCloud() bool {
	return c.isCloud
}

func (c *RuntimeContext) SetHttpEndpoint(endpoint string) {
	c.httpEndpoint = endpoint
}

func (c *RuntimeContext) SpicePath() (constants.SpiceInstallPath, string, error) {
	executableDir, err := os.Executable()
	if err != nil {
		return constants.OtherInstall, "", err
	}

	spiceBinDir := filepath.Join(c.SpiceRuntimeDir(), "bin")
	releaseFilePath := filepath.Join(spiceBinDir, constants.SpiceCliFilename)

	if executableDir == releaseFilePath {
		return constants.StandardInstall, executableDir, nil
	}

	brewPath := getBrewPrefix()
	if brewPath != "" && strings.Contains(executableDir, brewPath) {
		return constants.BrewInstall, executableDir, nil
	}

	return constants.OtherInstall, executableDir, nil
}

func getBrewPrefix() string {
	cmd := exec.Command("brew", "--prefix")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	brewPrefix := strings.TrimSpace(string(out))
	return brewPrefix
}

func loadDotEnvValues() (map[string]string, error) {
	env_file := ".env"
	if _, err := os.Stat(".env.local"); err == nil {
		env_file = ".env.local"
	} else if _, err := os.Stat(env_file); err != nil {
		return nil, nil
	}

	return godotenv.Read(env_file)
}
