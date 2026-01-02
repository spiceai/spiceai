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
	"fmt"
	"log/slog"
	"os"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

// APIKeysCmd shows API keys for an app
var APIKeysCmd = &cobra.Command{
	Use:     "api-keys",
	Aliases: []string{"apikeys", "keys"},
	Short:   "Manage API keys for an app",
	Example: `
# Show API keys for linked app
spice cloud api-keys

# Show API keys for a specific app
spice cloud api-keys --app myorg/myapp
spice cloud api-keys regenerate --app myorg/myapp
spice cloud api-keys regenerate --app myorg/myapp --key 2
`,
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		keys, err := client.GetAPIKeys(appName)
		if err != nil {
			slog.Error("Failed to get API keys", "error", err)
			os.Exit(1)
		}

		cmd.Println("API Keys:")
		if keys.APIKey != nil && *keys.APIKey != "" {
			cmd.Printf("  Primary:   %s\n", *keys.APIKey)
		} else {
			cmd.Println("  Primary:   (not set)")
		}
		if keys.APIKey2 != nil && *keys.APIKey2 != "" {
			cmd.Printf("  Secondary: %s\n", *keys.APIKey2)
		} else {
			cmd.Println("  Secondary: (not set)")
		}
	},
}

var regenerateAPIKeyCmd = &cobra.Command{
	Use:   "regenerate",
	Short: "Regenerate an API key for an app",
	Long: `Regenerate an API key for an app. This invalidates the previous key.

Use --key to specify which key to regenerate:
  0 - Both keys
  1 - Primary key (default)
  2 - Secondary key`,
	Example: `
# Regenerate API key for linked app
spice cloud api-keys regenerate

# Regenerate API key for a specific app
spice cloud api-keys regenerate --app myorg/myapp
spice cloud api-keys regenerate --app myorg/myapp --key 2
spice cloud api-keys regenerate --app myorg/myapp --key 0
`,
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		keyNumber, _ := cmd.Flags().GetInt("key")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		result, err := client.RegenerateAPIKey(appName, keyNumber)
		if err != nil {
			slog.Error("Failed to regenerate API key", "error", err)
			os.Exit(1)
		}

		var keyDesc string
		switch result.RegeneratedKey {
		case 2:
			keyDesc = "secondary"
		case 0:
			keyDesc = "both"
		default:
			keyDesc = "primary"
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully regenerated %s API key(s)", keyDesc)))
		cmd.Println("\nNew API Keys:")
		if result.APIKey != nil && *result.APIKey != "" {
			cmd.Printf("  Primary:   %s\n", *result.APIKey)
		}
		if result.APIKey2 != nil && *result.APIKey2 != "" {
			cmd.Printf("  Secondary: %s\n", *result.APIKey2)
		}
	},
}

func init() {
	// API keys flags
	APIKeysCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")

	// Regenerate flags
	regenerateAPIKeyCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	regenerateAPIKeyCmd.Flags().Int("key", 1, "Which key to regenerate (0=both, 1=primary, 2=secondary)")

	APIKeysCmd.AddCommand(regenerateAPIKeyCmd)
}
