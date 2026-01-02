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
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// SecretsCmd is the parent command for secrets operations
var SecretsCmd = &cobra.Command{
	Use:     "secrets",
	Aliases: []string{"secret"},
	Short:   "Manage secrets for an app",
	Long: `Manage secrets for a Spice Cloud app.

Secrets are encrypted values available to your Spice runtime at startup.
Secret values are always masked and cannot be read back after being set.`,
	Example: `
# List all secrets
spice cloud secrets list

# Set a secret
spice cloud secrets set DATABASE_PASSWORD "my-secret-password"

# Remove a secret
spice cloud secrets rm DATABASE_PASSWORD
`,
	Run: func(cmd *cobra.Command, args []string) {
		// Default to list
		SecretsListCmd.Run(cmd, args)
	},
}

// SecretsListCmd lists secrets
var SecretsListCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"ls"},
	Short:   "List secrets for an app",
	Example: `
spice cloud secrets list
spice cloud secrets list --app myorg/myapp
spice cloud secrets list --json
`,
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		jsonOutput, _ := cmd.Flags().GetBool("json")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		secrets, err := client.ListSecrets(appName)
		if err != nil {
			slog.Error("Failed to list secrets", "error", err)
			os.Exit(1)
		}

		if len(secrets) == 0 {
			if jsonOutput {
				cmd.Println("[]")
			} else {
				cmd.Println("No secrets found. Set one with 'spice cloud secrets set NAME VALUE'")
			}
			return
		}

		if jsonOutput {
			jsonBytes, err := json.MarshalIndent(secrets, "", "  ")
			if err != nil {
				slog.Error("Failed to marshal to JSON", "error", err)
				os.Exit(1)
			}
			cmd.Println(string(jsonBytes))
			return
		}

		var table []interface{}
		for _, secret := range secrets {
			table = append(table, secretTableRow{
				Name:      secret.Name,
				Value:     secret.Value, // Already masked by API
				CreatedAt: secret.CreatedAt,
				UpdatedAt: secret.UpdatedAt,
			})
		}
		util.WriteTable(table)
	},
}

type secretTableRow struct {
	Name      string `csv:"NAME"`
	Value     string `csv:"VALUE"`
	CreatedAt string `csv:"CREATED_AT"`
	UpdatedAt string `csv:"UPDATED_AT"`
}

// SecretsGetCmd gets a specific secret
var SecretsGetCmd = &cobra.Command{
	Use:   "get NAME",
	Short: "Get a secret by name",
	Long:  `Get a specific secret by name. The value is always masked.`,
	Example: `
spice cloud secrets get DATABASE_PASSWORD
spice cloud secrets get API_KEY --app myorg/myapp --json
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		name := args[0]
		jsonOutput, _ := cmd.Flags().GetBool("json")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		secret, err := client.GetSecret(appName, name)
		if err != nil {
			slog.Error("Failed to get secret", "error", err)
			os.Exit(1)
		}

		if jsonOutput {
			jsonBytes, err := json.MarshalIndent(secret, "", "  ")
			if err != nil {
				slog.Error("Failed to marshal to JSON", "error", err)
				os.Exit(1)
			}
			cmd.Println(string(jsonBytes))
			return
		}

		cmd.Printf("Name:       %s\n", secret.Name)
		cmd.Printf("Value:      %s\n", secret.Value)
		cmd.Printf("Created At: %s\n", secret.CreatedAt)
		cmd.Printf("Updated At: %s\n", secret.UpdatedAt)
	},
}

// SecretsSetCmd sets a secret
var SecretsSetCmd = &cobra.Command{
	Use:   "set NAME VALUE",
	Short: "Set a secret",
	Long: `Set a secret for a Spice Cloud app.

Creates a new secret or updates an existing one with the same name.
The value will be encrypted and cannot be read back.`,
	Example: `
# Set a secret
spice cloud secrets set DATABASE_PASSWORD "my-secret-password"

# Set from a file
cat secret.txt | spice cloud secrets set MY_SECRET -

# Set for a specific app
spice cloud secrets set API_KEY "sk-secret-key" --app myorg/myapp
`,
	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		name := args[0]
		value := args[1]

		// Support reading from stdin with "-"
		if value == "-" {
			scanner := bufio.NewScanner(os.Stdin)
			var lines []string
			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}
			value = strings.Join(lines, "\n")
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		_, err = client.SetSecret(appName, name, value)
		if err != nil {
			slog.Error("Failed to set secret", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully set secret '%s' for %s", name, appName)))
		cmd.Println("\nNote: Redeploy your app for the change to take effect.")
	},
}

// SecretsRemoveCmd removes a secret
var SecretsRemoveCmd = &cobra.Command{
	Use:     "rm NAME",
	Aliases: []string{"remove", "delete"},
	Short:   "Remove a secret",
	Example: `
spice cloud secrets rm DATABASE_PASSWORD
spice cloud secrets rm API_KEY --app myorg/myapp
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		name := args[0]
		yes, _ := cmd.Flags().GetBool("yes")

		if !yes {
			cmd.Printf("Remove secret '%s' from %s? [y/N]: ", name, appName)
			var response string
			_, _ = fmt.Scanln(&response)
			if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
				cmd.Println("Cancelled.")
				return
			}
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		if err := client.DeleteSecret(appName, name); err != nil {
			slog.Error("Failed to remove secret", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully removed secret '%s' from %s", name, appName)))
		cmd.Println("\nNote: Redeploy your app for the change to take effect.")
	},
}

func init() {
	// Parent command
	SecretsCmd.AddCommand(SecretsListCmd)
	SecretsCmd.AddCommand(SecretsGetCmd)
	SecretsCmd.AddCommand(SecretsSetCmd)
	SecretsCmd.AddCommand(SecretsRemoveCmd)

	// List flags
	SecretsListCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	SecretsListCmd.Flags().Bool("json", false, "Output as JSON")

	// Get flags
	SecretsGetCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	SecretsGetCmd.Flags().Bool("json", false, "Output as JSON")

	// Set flags
	SecretsSetCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")

	// Remove flags
	SecretsRemoveCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	SecretsRemoveCmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
}
