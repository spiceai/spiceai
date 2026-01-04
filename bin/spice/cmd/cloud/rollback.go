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
	"strconv"
	"strings"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// RollbackCmd rolls back to a previous deployment
var RollbackCmd = &cobra.Command{
	Use:   "rollback [deployment-id]",
	Short: "Rollback to a previous deployment",
	Long: `Rollback to a previous deployment.

If no deployment ID is provided, an interactive list of recent deployments
will be shown for selection. Only 'succeeded' deployments can be rolled back to.`,
	Example: `
# Interactive rollback (shows list of deployments)
spice cloud rollback

# Rollback to a specific deployment
spice cloud rollback 12345

# Skip confirmation
spice cloud rollback 12345 --yes
`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		yes, _ := cmd.Flags().GetBool("yes")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		var targetDeploymentID int64

		if len(args) > 0 {
			targetDeploymentID, err = strconv.ParseInt(args[0], 10, 64)
			if err != nil {
				slog.Error("Invalid deployment ID", "error", err)
				os.Exit(1)
			}
		} else {
			// Show interactive list
			deployments, err := client.ListDeployments(appName, 10, "succeeded")
			if err != nil {
				slog.Error("Failed to list deployments", "error", err)
				os.Exit(1)
			}

			if len(deployments) == 0 {
				slog.Error("No successful deployments found to rollback to")
				os.Exit(1)
			}

			cmd.Println("Select a deployment to rollback to:")
			cmd.Println()

			var table []interface{}
			for i, d := range deployments {
				table = append(table, rollbackTableRow{
					Index:        strconv.Itoa(i + 1),
					DeploymentID: strconv.FormatInt(d.ID, 10),
					ImageTag:     d.ImageTag,
					CommitSHA:    truncate(d.CommitSHA, 8),
					CreatedAt:    d.CreatedAt,
				})
			}
			util.WriteTable(table)

			cmd.Println()
			cmd.Print("Enter number (1-", len(deployments), ") or deployment ID: ")
			var input string
			_, _ = fmt.Scanln(&input)

			// Check if it's an index
			if idx, err := strconv.Atoi(input); err == nil && idx >= 1 && idx <= len(deployments) {
				targetDeploymentID = deployments[idx-1].ID
			} else if id, err := strconv.ParseInt(input, 10, 64); err == nil {
				targetDeploymentID = id
			} else {
				slog.Error("Invalid selection")
				os.Exit(1)
			}
		}

		// Get deployment details for confirmation
		deployment, err := client.GetDeployment(appName, targetDeploymentID)
		if err != nil {
			slog.Error("Failed to get deployment", "error", err)
			os.Exit(1)
		}

		if deployment.Status != "succeeded" {
			slog.Error("Can only rollback to successful deployments", "status", deployment.Status)
			os.Exit(1)
		}

		// Confirm rollback
		if !yes {
			cmd.Printf("\nRollback %s to deployment %d?\n", appName, targetDeploymentID)
			if deployment.ImageTag != "" {
				cmd.Printf("  Image Tag: %s\n", deployment.ImageTag)
			}
			if deployment.CommitSHA != "" {
				cmd.Printf("  Commit:    %s\n", deployment.CommitSHA)
			}
			if deployment.CreatedAt != "" {
				cmd.Printf("  Created:   %s\n", deployment.CreatedAt)
			}
			cmd.Print("\nProceed? [y/N]: ")

			var response string
			_, _ = fmt.Scanln(&response)
			if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
				cmd.Println("Cancelled.")
				return
			}
		}

		// Perform rollback
		newDeployment, err := client.Rollback(appName, targetDeploymentID)
		if err != nil {
			slog.Error("Failed to rollback", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully initiated rollback (new deployment ID: %d)", newDeployment.ID)))
		cmd.Printf("Status: %s\n", newDeployment.Status)
		cmd.Println("\nUse 'spice cloud inspect' to check progress or 'spice cloud logs -f' to follow logs.")
	},
}

type rollbackTableRow struct {
	Index        string `csv:"#"`
	DeploymentID string `csv:"DEPLOYMENT_ID"`
	ImageTag     string `csv:"IMAGE_TAG"`
	CommitSHA    string `csv:"COMMIT_SHA"`
	CreatedAt    string `csv:"CREATED_AT"`
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func init() {
	RollbackCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	RollbackCmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
}
