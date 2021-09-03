package config

import (
	"bytes"
	"fmt"
	"os"

	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/util"
	"gopkg.in/yaml.v2"
)

type SpiceConfiguration struct {
	HttpPort uint `json:"http_port,omitempty" mapstructure:"http_port,omitempty" yaml:"http_port,omitempty"`
}

func LoadDefaultConfiguration() *SpiceConfiguration {
	return &SpiceConfiguration{
		HttpPort: 8000,
	}
}

func LoadRuntimeConfiguration(v *viper.Viper, appDir string) (*SpiceConfiguration, error) {
	v.AddConfigPath(appDir)
	v.SetConfigName(constants.SpiceConfigBaseName)
	v.SetConfigType("yaml")

	var config *SpiceConfiguration
	configPath := fmt.Sprintf("%s.yaml", constants.SpiceConfigBaseName)

	if _, err := os.Stat(configPath); err != nil {
		configPath = fmt.Sprintf("%s.yml", constants.SpiceConfigBaseName)
		if _, err := os.Stat(configPath); err != nil {
			// No config file found, use defaults
			config = LoadDefaultConfiguration()
			return config, nil
		}
	}

	configBytes, err := util.ReplaceEnvVariablesFromPath(configPath, constants.SpiceEnvVarPrefix)
	if err != nil {
		return nil, err
	}

	err = v.ReadConfig(bytes.NewBuffer(configBytes))
	if err != nil {
		return nil, err
	}

	err = v.Unmarshal(&config)
	if err != nil {
		return nil, err
	}

	return config, err
}

func (rtConfig *SpiceConfiguration) ServerBaseUrl() string {
	return fmt.Sprintf("http://localhost:%d", rtConfig.HttpPort)
}

func (rtConfig *SpiceConfiguration) WriteToFile() error {
	configPath := fmt.Sprintf("%s.yaml", constants.SpiceConfigBaseName)
	configFile, err := os.Create(configPath)
	if err != nil {
		return fmt.Errorf("error creating %s: %w", configPath, err)
	}

	marshalledConfig, err := yaml.Marshal(rtConfig)
	if err != nil {
		return err
	}

	_, err = configFile.Write(marshalledConfig)
	if err != nil {
		return fmt.Errorf("error writing %s: %w", configPath, err)
	}

	err = configFile.Sync()
	if err != nil {
		return fmt.Errorf("error writing %s: %s", configPath, err)
	}

	return nil
}
