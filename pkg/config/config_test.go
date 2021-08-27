package config_test

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestConfig(t *testing.T) {
	testConfigPath := "../../test/assets/config/config.yaml"
	testConfigPathWithEnvVars := "../../test/assets/config/config_with_env_vars.yaml"
	t.Cleanup(testutils.CleanupTestSpiceDirectory)
	t.Run("LoadRuntimeConfiguration() - Config loads correctly", testRuntimeConfigLoads(testConfigPath))
	testutils.CleanupTestSpiceDirectory()
	t.Run("LoadRuntimeConfiguration() - Environment variables in config are replaced", testRuntimeConfigReplacesEnvironmentVariables(testConfigPathWithEnvVars))
}

// Tests configuration loads correctly
func testRuntimeConfigLoads(testConfigPath string) func(*testing.T) {
	return func(t *testing.T) {
		testutils.EnsureTestSpiceDirectory(t)

		tempConfigPath := filepath.Join(".spice", "config.yaml")
		copyFile(testConfigPath, tempConfigPath)

		viper := viper.New()
		rtcontext := context.CurrentContext()
		spiceConfiguration, err := config.LoadRuntimeConfiguration(viper, rtcontext.AppDir())
		if err != nil {
			t.Error(err)
			return
		}

		actual := strconv.Itoa(int(spiceConfiguration.HttpPort))
		expected := "8000"

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests configuration replaces environment variables correctly
func testRuntimeConfigReplacesEnvironmentVariables(testConfigPath string) func(*testing.T) {
	return func(t *testing.T) {
		testutils.EnsureTestSpiceDirectory(t)

		// Go 1.17 includes a Setenv on the testing pkg, but for now we will just set/unset with the os package
		testEnvVar := "SPICE_DASHBOARD_TO_REPLACE"
		if os.Getenv(testEnvVar) != "" {
			t.Errorf("%s must not be set during tests", testEnvVar)
		}

		expected := "replacedvalue"
		os.Setenv(testEnvVar, expected)

		tempConfigPath := filepath.Join(".spice", "config.yaml")
		copyFile(testConfigPath, tempConfigPath)

		viper := viper.New()
		rtcontext := context.CurrentContext()
		spiceConfiguration, err := config.LoadRuntimeConfiguration(viper, rtcontext.AppDir())
		if err != nil {
			t.Error(err)
			os.Unsetenv(testEnvVar)
			return
		}

		actual := *spiceConfiguration.CustomDashboardPath
		if !assert.Equal(t, expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
			os.Unsetenv(testEnvVar)
		}

		os.Unsetenv(testEnvVar)
	}
}

func copyFile(fromPath string, toPath string) {
	from, err := os.Open(fromPath)
	if err != nil {
		log.Fatal(err)
	}
	defer from.Close()

	to, err := os.OpenFile(toPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		log.Fatal(err)
	}
}
