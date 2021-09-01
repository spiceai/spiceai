package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/util"
	"gopkg.in/yaml.v2"
)

type SpiceConfiguration struct {
	HttpPort            uint    `json:"http_port,omitempty" mapstructure:"http_port,omitempty" yaml:"http_port,omitempty"`
	CustomDashboardPath *string `json:"custom_dashboard_path,omitempty" mapstructure:"custom_dashboard_path,omitempty" yaml:"custom_dashboard_path,omitempty"`
}

func LoadDefaultConfiguration() *SpiceConfiguration {
	return &SpiceConfiguration{
		HttpPort: 8000,
	}
}

func LoadRuntimeConfiguration(v *viper.Viper, appDir string) (*SpiceConfiguration, error) {
	spiceAppPath := filepath.Join(appDir, constants.DotSpice)
	v.AddConfigPath(spiceAppPath)
	v.SetConfigName("config")
	v.SetConfigType("yaml")

	var config *SpiceConfiguration
	configPath := ""

	if _, err := os.Stat(".spice/config.yaml"); err == nil {
		configPath = ".spice/config.yaml"
	} else if _, err := os.Stat(".spice/config.yml"); err == nil {
		configPath = ".spice/config.yml"
	}

	if configPath != "" {
		configBytes, err := util.ReplaceEnvVariablesFromPath(configPath, constants.SpiceEnvVarPrefix)
		if err != nil {
			return nil, err
		}

		err = v.ReadConfig(bytes.NewBuffer(configBytes))
		if err != nil {
			return nil, err
		}
	} else {
		_, err := util.MkDirAllInheritPerm(spiceAppPath)
		if err != nil {
			return nil, fmt.Errorf("error creating %s: %w", spiceAppPath, err)
		}

		fi, err := os.Stat(spiceAppPath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat %s: %w", spiceAppPath, err)
		}

		mode := fi.Mode()

		fmt.Print("Owner: ")
		for i := 1; i < 4; i++ {
			fmt.Print(string(mode.String()[i]))
		}

		fmt.Print("\nGroup: ")
		for i := 4; i < 7; i++ {
			fmt.Print(string(mode.String()[i]))
		}

		fmt.Print("\nOther: ")
		for i := 7; i < 10; i++ {
			fmt.Print(string(mode.String()[i]))
		}

		configPath := filepath.Join(spiceAppPath, "config.yaml")

		configFile, err := os.Create(configPath)
		if err != nil {
			return nil, fmt.Errorf("error creating %s: %w", configPath, err)
		}

		// No config file found, use defaults
		config = LoadDefaultConfiguration()
		marshalledConfig, err := yaml.Marshal(config)
		if err != nil {
			return nil, err
		}

		_, err = configFile.Write(marshalledConfig)
		if err != nil {
			return nil, fmt.Errorf("error writing %s: %w", configPath, err)
		}

		err = configFile.Sync()
		if err != nil {
			return nil, fmt.Errorf("error initializing .spice/config.yaml: %s", err)
		}
	}

	v.WatchConfig()

	err := v.Unmarshal(&config)
	return config, err
}

func (rtConfig *SpiceConfiguration) ServerBaseUrl() string {
	return fmt.Sprintf("http://localhost:%d", rtConfig.HttpPort)
}
