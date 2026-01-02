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

package cloud

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	// CloudConfigDir is the directory for cloud configuration
	CloudConfigDir = ".spice"
	// CloudConfigFile is the filename for cloud link configuration
	CloudConfigFile = "cloud.json"
)

// CloudLink represents the link configuration for a cloud app
type CloudLink struct {
	Org      string `json:"org"`
	App      string `json:"app"`
	AppID    int64  `json:"app_id,omitempty"`
	Region   string `json:"region,omitempty"`
	LinkedAt string `json:"linked_at,omitempty"`
}

// FullName returns the full app name in org/app format
func (l *CloudLink) FullName() string {
	return fmt.Sprintf("%s/%s", l.Org, l.App)
}

// GetCloudConfigPath returns the path to the cloud config file
func GetCloudConfigPath() string {
	return filepath.Join(CloudConfigDir, CloudConfigFile)
}

// LoadCloudLink loads the cloud link configuration from the current directory
func LoadCloudLink() (*CloudLink, error) {
	configPath := GetCloudConfigPath()

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No link exists
		}
		return nil, fmt.Errorf("failed to read cloud config: %w", err)
	}

	var link CloudLink
	if err := json.Unmarshal(data, &link); err != nil {
		return nil, fmt.Errorf("failed to parse cloud config: %w", err)
	}

	return &link, nil
}

// SaveCloudLink saves the cloud link configuration to the current directory
func SaveCloudLink(link *CloudLink) error {
	// Create .spice directory if it doesn't exist
	if err := os.MkdirAll(CloudConfigDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	data, err := json.MarshalIndent(link, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cloud config: %w", err)
	}

	configPath := GetCloudConfigPath()
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cloud config: %w", err)
	}

	return nil
}

// RemoveCloudLink removes the cloud link configuration from the current directory
func RemoveCloudLink() error {
	configPath := GetCloudConfigPath()

	if err := os.Remove(configPath); err != nil {
		if os.IsNotExist(err) {
			return nil // Already unlinked
		}
		return fmt.Errorf("failed to remove cloud config: %w", err)
	}

	// Try to remove the .spice directory if it's empty
	_ = os.Remove(CloudConfigDir)

	return nil
}

// GetLinkedApp returns the linked app name if available, otherwise returns the provided flag value
// This allows commands to use the linked app as a default when --app is not specified
func GetLinkedApp(flagValue string) (string, error) {
	// If a flag value is provided, use it
	if flagValue != "" {
		return flagValue, nil
	}

	// Try to load the linked app
	link, err := LoadCloudLink()
	if err != nil {
		return "", err
	}

	if link == nil {
		return "", nil // No link, no flag value
	}

	return link.FullName(), nil
}

// RequireApp returns the app name from flag or link, or returns an error if neither is available
func RequireApp(flagValue string) (string, error) {
	appName, err := GetLinkedApp(flagValue)
	if err != nil {
		return "", err
	}

	if appName == "" {
		return "", fmt.Errorf("app name is required. Use --app <org/app> or run 'spice cloud link' to link an app")
	}

	return appName, nil
}
