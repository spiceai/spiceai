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
	"log/slog"
	"os"
	"strconv"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

// InspectCmd shows detailed information about a deployment
var InspectCmd = &cobra.Command{
	Use:     "inspect [deployment-id]",
	Aliases: []string{"describe", "show"},
	Short:   "Show detailed information about a deployment",
	Long: `Show detailed information about a deployment including configuration,
status, timing, and error messages if any.

If no deployment ID is provided, shows the latest deployment.`,
	Example: `
# Inspect the latest deployment
spice cloud inspect

# Inspect a specific deployment
spice cloud inspect 12345

# Output as JSON
spice cloud inspect --json
spice cloud inspect 12345 --json
`,
	Args: cobra.MaximumNArgs(1),
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

		var deployment *api.Deployment
		if len(args) > 0 {
			deploymentID, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				slog.Error("Invalid deployment ID", "error", err)
				os.Exit(1)
			}
			deployment, err = client.GetDeployment(appName, deploymentID)
			if err != nil {
				slog.Error("Failed to get deployment", "error", err)
				os.Exit(1)
			}
		} else {
			deployment, err = client.GetLatestDeployment(appName)
			if err != nil {
				slog.Error("Failed to get latest deployment", "error", err)
				os.Exit(1)
			}
		}

		if jsonOutput {
			jsonBytes, err := json.MarshalIndent(deployment, "", "  ")
			if err != nil {
				slog.Error("Failed to marshal to JSON", "error", err)
				os.Exit(1)
			}
			cmd.Println(string(jsonBytes))
			return
		}

		// Pretty print deployment details
		cmd.Println(aurora.Bold("Deployment Details"))
		cmd.Println(aurora.Gray(12, "─────────────────────────────────────"))
		cmd.Printf("  %-16s %d\n", aurora.Cyan("ID:"), deployment.ID)
		cmd.Printf("  %-16s %s\n", aurora.Cyan("App:"), appName)
		cmd.Printf("  %-16s %s\n", aurora.Cyan("Status:"), formatStatus(deployment.Status))
		cmd.Println()

		cmd.Println(aurora.Bold("Configuration"))
		cmd.Println(aurora.Gray(12, "─────────────────────────────────────"))
		if deployment.ImageTag != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Image Tag:"), deployment.ImageTag)
		}
		if deployment.Replicas > 0 {
			cmd.Printf("  %-16s %d\n", aurora.Cyan("Replicas:"), deployment.Replicas)
		}
		if deployment.CreationSource != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Source:"), deployment.CreationSource)
		}
		cmd.Println()

		cmd.Println(aurora.Bold("Git Info"))
		cmd.Println(aurora.Gray(12, "─────────────────────────────────────"))
		if deployment.CommitSHA != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Commit SHA:"), deployment.CommitSHA)
		} else {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Commit SHA:"), aurora.Gray(12, "(not set)"))
		}
		if deployment.CommitMessage != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Message:"), deployment.CommitMessage)
		}
		cmd.Println()

		cmd.Println(aurora.Bold("Timing"))
		cmd.Println(aurora.Gray(12, "─────────────────────────────────────"))
		if deployment.CreatedAt != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Created:"), deployment.CreatedAt)
		}
		if deployment.StartedAt != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Started:"), deployment.StartedAt)
		}
		if deployment.FinishedAt != "" {
			cmd.Printf("  %-16s %s\n", aurora.Cyan("Finished:"), deployment.FinishedAt)
		}

		if deployment.ErrorMessage != "" {
			cmd.Println()
			cmd.Println(aurora.Bold(aurora.Red("Error")))
			cmd.Println(aurora.Gray(12, "─────────────────────────────────────"))
			cmd.Printf("  %s\n", deployment.ErrorMessage)
		}
	},
}

func formatStatus(status string) aurora.Value {
	switch status {
	case "succeeded":
		return aurora.BrightGreen(status)
	case "failed":
		return aurora.BrightRed(status)
	case "in_progress":
		return aurora.BrightYellow(status)
	case "queued", "created":
		return aurora.BrightCyan(status)
	default:
		return aurora.White(status)
	}
}

func init() {
	InspectCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	InspectCmd.Flags().Bool("json", false, "Output as JSON")
}
