package runtime

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/context"
)

var (
	spicedDockerImg string = "ghcr.io/spiceai/spiced:%s"
	spicedDockerCmd string = "run -p %d:%d %s --add-host=host.docker.internal:host-gateway -v %s:/userapp --rm %s"
)

func getSpiceEnvVarsAsDockerArgs() string {
	var dockerEnvArgs []string
	for _, envVar := range os.Environ() {
		if strings.HasPrefix(envVar, config.SpiceEnvVarPrefix) {
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

func getHttpPort() (uint, error) {
	v := viper.New()
	runtimeConfig, err := config.LoadRuntimeConfiguration(v)
	if err != nil {
		fmt.Println("failed to load runtime configuration")
		return 0, err
	}

	return runtimeConfig.HttpPort, nil
}

func Run(cliContext context.RuntimeContext, manifestPath string) error {
	fmt.Println("Spice runtime starting...")

	var cmd *exec.Cmd

	switch cliContext {
	case context.Docker:
		userApp, err := os.Getwd()
		if err != nil {
			return err
		}

		dockerVersionTagBytes, err := os.ReadFile(GetDockerVersionFilePath())
		if err != nil {
			return err
		}
		dockerVersion := strings.TrimSpace(string(dockerVersionTagBytes))

		httpPort, err := getHttpPort()
		if err != nil {
			return err
		}

		spiceEnvArgs := getSpiceEnvVarsAsDockerArgs()

		dockerImg := fmt.Sprintf(spicedDockerImg, dockerVersion)
		dockerArgs := getDockerArgs(fmt.Sprintf(spicedDockerCmd, httpPort, httpPort, spiceEnvArgs, userApp, dockerImg))

		if manifestPath != "" {
			dockerArgs = append(dockerArgs, manifestPath)
		}

		cmd = exec.Command("docker", dockerArgs...)

	case context.BareMetal:
		err := EnsureAIPresent()
		if err != nil {
			return fmt.Errorf("AI Engine has not been downloaded")
		}
		spiceCMD := binaryFilePath(config.SpiceBinPath(), "spiced")

		cmd = exec.Command(spiceCMD, manifestPath)

	default:
		return fmt.Errorf("unknown runtime context: %v", cliContext)
	}

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err := cmd.Start()
	if err != nil {
		return err
	}

	return cmd.Wait()
}
