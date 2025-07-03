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
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/github"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
	"golang.org/x/mod/semver"
)

const (
	GET  = "GET"
	POST = "POST"
)

type RuntimeContext struct {
	spiceRuntimeDir string
	flags           *pflag.FlagSet
	spiceBinDir     string
	appDir          string
	podsDir         string
	isCloud         bool
	httpClient      *http.Client
	userAgent       string
	extraHeaders    map[string]string
}

func NewContext() *RuntimeContext {
	rtcontext := &RuntimeContext{
		httpClient:   &http.Client{},
		userAgent:    util.GetSpiceUserAgent("spice"),
		extraHeaders: make(map[string]string),
	}
	return rtcontext
}

func FromFlags(flags *pflag.FlagSet) (*RuntimeContext, error) {
	ctx := NewContext()
	if err := ctx.Init(flags); err != nil {
		return nil, err
	}
	return ctx, nil
}

func TlsHttpClient(rootCertPath string) http.Client {
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

	return http.Client{
		Transport: transport,
	}
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
	if c.IsCloud() {
		return "https://data.spiceai.io"
	}

	if endpoint, err := c.flags.GetString(constants.HttpEndpointKeyFlag); err == nil && endpoint != "" {
		return endpoint
	}

	return "http://127.0.0.1:8090"
}

func (c *RuntimeContext) Do(method, path string, body io.Reader, additionalHeaders ...string) (*http.Response, error) {
	request, err := http.NewRequest(method, fmt.Sprintf("%s%s", c.HttpEndpoint(), path), body)
	if err != nil {
		return nil, fmt.Errorf("error sending HTTP request: %w", err)
	}

	headers := c.GetHeaders()
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	for i := 0; i < len(additionalHeaders); i += 2 {
		request.Header.Set(additionalHeaders[i], additionalHeaders[i+1])
	}

	return c.httpClient.Do(request)
}

func (c *RuntimeContext) HttpSocketAddress() string {
	if endpoint, err := c.flags.GetString("http-endpoint"); err == nil && endpoint != "" {
		return endpoint
	}

	if c.IsCloud() {
		slog.Warn("Attempting to get socket address for HTTP endpoint when `--cloud` enabled")
	}

	// Note socket address, not HTTP address
	return "127.0.0.1:8090"

}

func (c *RuntimeContext) Init(flags *pflag.FlagSet) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	c.spiceRuntimeDir = filepath.Join(homeDir, constants.DotSpice)
	c.spiceBinDir = filepath.Join(c.spiceRuntimeDir, "bin")
	c.flags = flags

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
		if err := flags.Set(constants.ApiKeyFlag, apiKey); err != nil {
			return fmt.Errorf("failed to set api-key flag from SPICE_SPICEAI_API_KEY environment variable: %w", err)
		}
	}

	client := http.Client{}
	rootCertPath, err := flags.GetString(constants.TlsRootCertificateFile)
	if err != nil {
		return err
	}
	cloud, _ := flags.GetBool(constants.CloudKeyFlag)
	c.isCloud = cloud
	if rootCertPath != "" {
		client = TlsHttpClient(rootCertPath)
	}
	c.httpClient = &client
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
	err := c.InstallMatchingRuntime(constants.FlavorAI, true) // default to using an accelerator for prompted installs
	if err != nil {
		slog.Error("installing models runtime", "error", err)
		os.Exit(1)
	}
}

func (c *RuntimeContext) EnsureInstalled(flavor constants.Flavor, autoUpgrade bool, allowAccelerator bool) (bool, error) {
	if !flavor.IsValid() {
		return false, fmt.Errorf("invalid flavor")
	}

	shouldInstall := false
	var err error
	var upgradeVersion string
	if installRequired := c.IsRuntimeInstallRequired(); installRequired {
		slog.Info("Spice runtime installation required")
		shouldInstall = true
	} else {
		upgradeVersion, err = c.IsRuntimeUpgradeAvailable()
		if err != nil {
			slog.Warn("error checking for runtime upgrade", "error", err)
		} else if upgradeVersion != "" && autoUpgrade {
			shouldInstall = true
		}
	}

	if models, _ := c.ModelsFlavorInstalled(); !models && flavor == constants.FlavorAI {
		shouldInstall = true
	}

	if shouldInstall {
		err = c.InstallMatchingRuntime(flavor, allowAccelerator)
		if err != nil {
			return shouldInstall, err
		}
	}

	return shouldInstall, nil
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
	return fmt.Errorf("the Spice runtime is unavailable at %s. Is it running?", c.HttpEndpoint())
}

func (c *RuntimeContext) IsRuntimeInstallRequired() bool {
	binaryPath := c.binaryFilePath(constants.SpiceRuntimeFilename)

	// first time install?
	_, err := os.Stat(binaryPath)
	return errors.Is(err, os.ErrNotExist)
}

func (c *RuntimeContext) InstallMatchingRuntime(flavor constants.Flavor, allowAccelerator bool) error {
	cliVersion := version.Version()
	err := c.prepareInstallDir()
	if err != nil {
		return err
	}

	release, err := github.GetRuntimeRelease(cliVersion)
	if err != nil {
		return err
	}

	slog.Info(fmt.Sprintf("Downloading and installing Spice.ai Runtime %s ...\n", release.TagName))

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

	cliVersion := version.Version()

	if semver.Compare(currentVersion, cliVersion) >= 0 {
		return "", nil
	}

	return cliVersion, nil
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
		"--pods-watcher-enabled",
	}

	args = append(spiceArgs, c.getRuntimeArgsFromFlags(args)...)

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
	c.isCloud = isCloud
	return c
}

func (c *RuntimeContext) GetApiKey() (string, error) {
	return c.flags.GetString(constants.ApiKeyFlag)
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

	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("SPICE_SPICEAI_API_KEY")
	}
	if apiKey != "" {
		headers["X-API-Key"] = apiKey
	}

	// api_key from context takes precedence
	if cmdApiKey, err := c.GetApiKey(); err == nil && cmdApiKey != "" {
		headers["X-API-Key"] = cmdApiKey
	}

	for key, value := range c.extraHeaders {
		headers[key] = value
	}

	return headers
}

func (c *RuntimeContext) IsCloud() bool {
	return c.isCloud
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

func (c *RuntimeContext) configureFlag(args []string, flag string, defaultValue string) []string {
	if value, err := c.flags.GetString(flag); err == nil && value != "" {
		return append(args, "--"+flag, value)
	} else {
		if defaultValue == "" {
			return args
		}
		return append(args, "--"+flag, defaultValue)
	}
}

func (c *RuntimeContext) configureFlightFlag(args []string) []string {
	if flight, err := c.flags.GetString("flight-endpoint"); err == nil && flight != "" {
		if slices.Contains(args, "--repl") {
			args = append(args, "--repl-flight-endpoint", flight)
		} else {
			args = appendIfAbsent(args, "--flight", flight)
		}
	}
	return args
}

func (c *RuntimeContext) configureMetricsFlag(args []string) []string {
	if metricsEndpoint, err := c.flags.GetString("metrics-endpoint"); err == nil && metricsEndpoint != "" {
		args = appendIfAbsent(args, "--metrics", metricsEndpoint)
	}
	return args
}

func (c *RuntimeContext) configureEndpoints(args []string) []string {
	args = c.configureFlightFlag(args)
	args = appendIfAbsent(args, "--http", c.HttpSocketAddress())
	args = c.configureMetricsFlag(args)
	return args
}

func (c *RuntimeContext) configureUserAgent(args []string) []string {
	if c.userAgent != "" {
		args = append(args, "--user-agent", c.userAgent)
	} else if userAgent, err := c.flags.GetString(constants.UserAgentKeyFlag); err == nil && userAgent != "" {
		args = append(args, "--user-agent", userAgent)
	}
	return args
}

func (c *RuntimeContext) configureCapturedOutput(args []string) []string {
	if capturedOutput, err := c.flags.GetString("captured-output"); err == nil && capturedOutput != "" {
		args = append(args, "--set-runtime", "task_history.captured_output="+capturedOutput)
	} else {
		// Set the default value for captured_output to truncated to provide a better local developer experience with spice trace.
		args = append(args, "--set-runtime", "task_history.captured_output=truncated")
	}
	return args
}

func (c *RuntimeContext) getRuntimeArgsFromFlags(args []string) []string {
	args = c.configureFlag(args, constants.TlsRootCertificateFile, "")
	args = c.configureFlag(args, constants.ApiKeyFlag, "")
	args = c.configureUserAgent(args)
	args = c.configureFlag(args, "cache-control", "")
	args = c.configureEndpoints(args)
	args = c.configureCapturedOutput(args)
	return args
}

func appendIfAbsent(args []string, flag string, value string) []string {
	if !slices.Contains(args, flag) {
		return append(args, flag, value)
	}
	return args
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
