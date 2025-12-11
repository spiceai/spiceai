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

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// DeploymentsCmd lists deployments for an app
var DeploymentsCmd = &cobra.Command{
	Use:     "deployments",
	Aliases: []string{"deployment", "list-deployments"},
	Short:   "List deployments for an app",
	Example: `
# List deployments for linked app
spice cloud deployments

# List deployments for a specific app
spice cloud deployments --app myorg/myapp
spice cloud deployments --app myorg/myapp --limit 10 --status succeeded
`,
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		limit, _ := cmd.Flags().GetInt("limit")
		status, _ := cmd.Flags().GetString("status")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		deployments, err := client.ListDeployments(appName, limit, status)
		if err != nil {
			slog.Error("Failed to list deployments", "error", err)
			os.Exit(1)
		}

		if len(deployments) == 0 {
			cmd.Println("No deployments found. Create one with 'spice cloud deploy --app <org/app>'")
			return
		}

		var table []interface{}
		for _, d := range deployments {
			table = append(table, deploymentTableRow{
				ID:        strconv.FormatInt(d.ID, 10),
				Status:    d.Status,
				ImageTag:  d.ImageTag,
				Replicas:  strconv.Itoa(d.Replicas),
				CreatedAt: d.CreatedAt,
			})
		}
		util.WriteTable(table)
	},
}

type deploymentTableRow struct {
	ID        string `csv:"ID"`
	Status    string `csv:"STATUS"`
	ImageTag  string `csv:"IMAGE_TAG"`
	Replicas  string `csv:"REPLICAS"`
	CreatedAt string `csv:"CREATED_AT"`
}

// DeployCmd creates a new deployment
var DeployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "Create a new deployment for an app",
	Example: `
# Deploy linked app
spice cloud deploy

# Deploy a specific app
spice cloud deploy --app myorg/myapp
spice cloud deploy --app myorg/myapp --image-tag 1.5.0-models --replicas 2
spice cloud deploy --app myorg/myapp --commit-sha abc123 --commit-message "Deploy new feature"
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

		req := &api.CreateDeploymentRequest{}

		if imageTag, _ := cmd.Flags().GetString("image-tag"); imageTag != "" {
			req.ImageTag = imageTag
		}
		if replicas, _ := cmd.Flags().GetInt("replicas"); replicas > 0 {
			req.Replicas = replicas
		}
		if branch, _ := cmd.Flags().GetString("branch"); branch != "" {
			req.Branch = branch
		}
		if commitSHA, _ := cmd.Flags().GetString("commit-sha"); commitSHA != "" {
			req.CommitSHA = commitSHA
		}
		if commitMsg, _ := cmd.Flags().GetString("commit-message"); commitMsg != "" {
			req.CommitMessage = commitMsg
		}
		if debug, _ := cmd.Flags().GetBool("debug"); debug {
			req.Debug = debug
		}

		deployment, err := client.CreateDeployment(appName, req)
		if err != nil {
			slog.Error("Failed to create deployment", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully created deployment (ID: %d)", deployment.ID)))
		cmd.Printf("Status: %s\n", deployment.Status)
		if deployment.ImageTag != "" {
			cmd.Printf("Image:  %s\n", deployment.ImageTag)
		}
		if deployment.Replicas > 0 {
			cmd.Printf("Replicas: %d\n", deployment.Replicas)
		}
	},
}

var createDeploymentCmd = &cobra.Command{
	Use:   "deployment",
	Short: "Create a new deployment for an app",
	Example: `
spice cloud create deployment --app myorg/myapp
spice cloud create deployment --app myorg/myapp --image-tag 1.5.0-models
`,
	Run: DeployCmd.Run,
}

func init() {
	// Deployments list flags
	DeploymentsCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	DeploymentsCmd.Flags().Int("limit", 20, "Maximum number of deployments to return")
	DeploymentsCmd.Flags().String("status", "", "Filter by status (queued, in_progress, succeeded, failed, created)")

	// Deploy flags
	DeployCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	DeployCmd.Flags().String("image-tag", "", "Override the spice.ai runtime image tag")
	DeployCmd.Flags().Int("replicas", 0, "Override the number of replicas (1-10)")
	DeployCmd.Flags().String("branch", "", "Git branch name")
	DeployCmd.Flags().String("commit-sha", "", "Git commit SHA")
	DeployCmd.Flags().String("commit-message", "", "Git commit message")
	DeployCmd.Flags().Bool("debug", false, "Enable debug mode")

	// Create deployment flags (mirrors deploy)
	createDeploymentCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	createDeploymentCmd.Flags().String("image-tag", "", "Override the spice.ai runtime image tag")
	createDeploymentCmd.Flags().Int("replicas", 0, "Override the number of replicas (1-10)")
	createDeploymentCmd.Flags().String("branch", "", "Git branch name")
	createDeploymentCmd.Flags().String("commit-sha", "", "Git commit SHA")
	createDeploymentCmd.Flags().String("commit-message", "", "Git commit message")
	createDeploymentCmd.Flags().Bool("debug", false, "Enable debug mode")
}
