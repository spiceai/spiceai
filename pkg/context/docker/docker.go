package docker

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/constants"
	"github.com/spiceai/spice/pkg/github"
)

type DockerContext struct {
	spiceBinDir string
	podsDir     string
}

const (
	spicedDockerImg        = "ghcr.io/spiceai/spiced"
	spicedDockerCmd        = "run -p %d:%d %s --add-host=host.docker.internal:host-gateway -v %s:/userapp --rm %s"
	dockerAppPath          = "/userapp"
	dockerSpiceRuntimePath = "/.spice"
	dockerAiEnginePath     = "/app/ai"
)

func NewDockerContext() *DockerContext {
	spiceBinDir := path.Join(dockerSpiceRuntimePath, "bin")
	podsDir := path.Join(dockerSpiceRuntimePath, "pods")

	return &DockerContext{
		spiceBinDir: spiceBinDir,
		podsDir:     podsDir,
	}
}

func (c *DockerContext) Version() (string, error) {
	version, err := getDockerImageVersion()
	if err != nil {
		return "", err
	}

	if version == "" {
		// Image doesn't exist, no local version yet
		release, err := github.GetLatestRuntimeRelease()
		if err != nil {
			return "", err
		}
		return github.GetRuntimeVersion(release), nil
	}

	return version, nil
}

func (c *DockerContext) SpiceRuntimeDir() string {
	return dockerSpiceRuntimePath
}

func (c *DockerContext) AIEngineDir() string {
	return dockerAiEnginePath
}

func (c *DockerContext) AIEnginePythonCmdPath() string {
	return constants.PythonCmd
}

func (c *DockerContext) AppDir() string {
	return dockerAppPath
}

func (c *DockerContext) PodsDir() string {
	return c.podsDir
}

func (c *DockerContext) Init() error {
	return nil
}

func (c *DockerContext) IsRuntimeInstallRequired() bool {
	version, err := getDockerImageVersion()
	if err != nil {
		return true
	}

	return version == ""
}

func (c *DockerContext) InstallOrUpgradeRuntime() error {
	// Docker run will "install" the image automatically
	return nil
}

func (c *DockerContext) IsRuntimeUpgradeAvailable() (string, error) {
	// Docker run will "upgrade" the image automatically
	return "", nil
}

func (c *DockerContext) GetRunCmd(manifestPath string) (*exec.Cmd, error) {
	version, err := getDockerImageVersion()
	if err != nil {
		return nil, err
	}

	if version == "dev" {
		fmt.Println("found and using local dev image")
	} else {
		version, err = c.Version()
		if err != nil {
			return nil, err
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	v := viper.New()
	config, err := config.LoadRuntimeConfiguration(v, cwd)
	if err != nil {
		return nil, err
	}

	spiceEnvArgs := getSpiceEnvVarsAsDockerArgs()

	dockerImg := fmt.Sprintf("%s:%s", spicedDockerImg, version)
	dockerArgs := getDockerArgs(fmt.Sprintf(spicedDockerCmd, config.HttpPort, config.HttpPort, spiceEnvArgs, cwd, dockerImg))

	if manifestPath != "" {
		dockerArgs = append(dockerArgs, manifestPath)
	}

	cmd := exec.Command("docker", dockerArgs...)

	return cmd, nil
}

func (c *DockerContext) GetSpiceAppRelativePath(absolutePath string) string {
	if strings.HasPrefix(absolutePath, dockerAppPath) {
		return absolutePath[len(dockerAppPath)+1:]
	}
	return absolutePath
}

func getSpiceEnvVarsAsDockerArgs() string {
	var dockerEnvArgs []string
	for _, envVar := range os.Environ() {
		if strings.HasPrefix(envVar, constants.SpiceEnvVarPrefix) {
			dockerEnvArgs = append(dockerEnvArgs, "--env")
			dockerEnvArgs = append(dockerEnvArgs, envVar)
		}
	}

	return strings.Join(dockerEnvArgs, " ")
}

func getDockerArgs(args string) []string {
	originalArgs := strings.Split(args, " ")

	// strings.Split will add empty strings if more than one space occurs in a row - trim them out
	var argsTrimmedOfEmptyStrings []string
	for _, arg := range originalArgs {
		if arg != "" {
			argsTrimmedOfEmptyStrings = append(argsTrimmedOfEmptyStrings, arg)
		}
	}

	return argsTrimmedOfEmptyStrings
}

func getDockerImageVersion() (string, error) {
	cmd := exec.Command("docker", "images", spicedDockerImg, "--format", "{{.Tag}}")

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	reader := bytes.NewReader(output)
	images, err := ioutil.ReadAll(reader)
	if err != nil {
		return "", err
	}

	if len(images) == 0 {
		return "", nil
	}

	lines := strings.Split(strings.TrimSpace(string(images)), "\n")
	if len(lines) == 0 {
		return "", nil
	}

	// Most recent image
	tag := strings.TrimSpace(lines[0])

	return tag, nil
}
