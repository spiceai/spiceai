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
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// AppsCmd lists all apps
var AppsCmd = &cobra.Command{
	Use:     "apps",
	Aliases: []string{"app", "list-apps"},
	Short:   "List all apps in Spice Cloud",
	Example: `
spice cloud apps
`,
	Run: func(cmd *cobra.Command, args []string) {
		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		apps, err := client.ListApps()
		if err != nil {
			slog.Error("Failed to list apps", "error", err)
			os.Exit(1)
		}

		if len(apps) == 0 {
			cmd.Println("No apps found. Create one with 'spice cloud create app <name>'")
			return
		}

		// Convert to table format
		var table []interface{}
		for _, app := range apps {
			table = append(table, appTableRow{
				Name:       app.FullName(),
				Region:     app.Region,
				Visibility: app.Visibility,
				CreatedAt:  app.CreatedAt,
			})
		}
		util.WriteTable(table)
	},
}

type appTableRow struct {
	Name       string `csv:"NAME"`
	Region     string `csv:"REGION"`
	Visibility string `csv:"VISIBILITY"`
	CreatedAt  string `csv:"CREATED_AT"`
}

var createAppCmd = &cobra.Command{
	Use:   "app <name>",
	Short: "Create a new app in Spice Cloud",
	Args:  cobra.ExactArgs(1),
	Example: `
spice cloud create app my-app
spice cloud create app my-app --description "My application" --visibility private
`,
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		description, _ := cmd.Flags().GetString("description")
		visibility, _ := cmd.Flags().GetString("visibility")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		req := &api.CreateAppRequest{
			Name:        name,
			Description: description,
			Visibility:  visibility,
		}

		app, err := client.CreateApp(req)
		if err != nil {
			slog.Error("Failed to create app", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully created app '%s' (ID: %d)", app.Name, app.ID)))
		if app.Region != "" {
			cmd.Printf("Region: %s\n", app.Region)
		}
	},
}

var getAppCmd = &cobra.Command{
	Use:   "app [org/app]",
	Short: "Get details of an app",
	Args:  cobra.MaximumNArgs(1),
	Example: `
# Get linked app details
spice cloud get app

# Get a specific app
spice cloud get app myorg/myapp
`,
	Run: func(cmd *cobra.Command, args []string) {
		var argValue string
		if len(args) > 0 {
			argValue = args[0]
		}
		appName, err := RequireApp(argValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		app, err := client.GetApp(appName)
		if err != nil {
			slog.Error("Failed to get app", "error", err)
			os.Exit(1)
		}

		cmd.Printf("Name:        %s\n", app.FullName())
		cmd.Printf("Description: %s\n", app.Description)
		cmd.Printf("Visibility:  %s\n", app.Visibility)
		cmd.Printf("Region:      %s\n", app.Region)
		cmd.Printf("Created:     %s\n", app.CreatedAt)
		if app.ProductionBranch != "" {
			cmd.Printf("Branch:      %s\n", app.ProductionBranch)
		}
		if app.Config != nil {
			cmd.Println("\nConfiguration:")
			if app.Config.ImageTag != "" {
				cmd.Printf("  Image Tag: %s\n", app.Config.ImageTag)
			}
			if app.Config.Replicas > 0 {
				cmd.Printf("  Replicas:  %d\n", app.Config.Replicas)
			}
			if app.Config.NodeGroup != "" {
				cmd.Printf("  Node Group: %s\n", app.Config.NodeGroup)
			}
		}
	},
}

var updateAppCmd = &cobra.Command{
	Use:   "app [org/app]",
	Short: "Update an app",
	Args:  cobra.MaximumNArgs(1),
	Example: `
# Update linked app
spice cloud update app --description "Updated description"

# Update a specific app
spice cloud update app myorg/myapp --description "Updated description"
spice cloud update app myorg/myapp --replicas 2 --image-tag 1.5.0-models
`,
	Run: func(cmd *cobra.Command, args []string) {
		var argValue string
		if len(args) > 0 {
			argValue = args[0]
		}
		appName, err := RequireApp(argValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		req := &api.UpdateAppRequest{}

		if description, _ := cmd.Flags().GetString("description"); description != "" {
			req.Description = description
		}
		if visibility, _ := cmd.Flags().GetString("visibility"); visibility != "" {
			req.Visibility = visibility
		}
		if branch, _ := cmd.Flags().GetString("production-branch"); branch != "" {
			req.ProductionBranch = branch
		}
		if imageTag, _ := cmd.Flags().GetString("image-tag"); imageTag != "" {
			req.ImageTag = imageTag
		}
		if replicas, _ := cmd.Flags().GetInt("replicas"); replicas > 0 {
			req.Replicas = replicas
		}
		if region, _ := cmd.Flags().GetString("region"); region != "" {
			req.Region = region
		}

		app, err := client.UpdateApp(appName, req)
		if err != nil {
			slog.Error("Failed to update app", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully updated app '%s'", app.FullName())))
	},
}

var deleteAppCmd = &cobra.Command{
	Use:   "app [org/app]",
	Short: "Delete an app",
	Args:  cobra.MaximumNArgs(1),
	Example: `
# Delete linked app
spice cloud delete app

# Delete a specific app
spice cloud delete app myorg/myapp
`,
	Run: func(cmd *cobra.Command, args []string) {
		var argValue string
		if len(args) > 0 {
			argValue = args[0]
		}
		appName, err := RequireApp(argValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		force, _ := cmd.Flags().GetBool("force")
		if !force {
			cmd.Printf("Are you sure you want to delete '%s'? This action cannot be undone. [y/N]: ", appName)
			var response string
			_, _ = fmt.Scanln(&response)
			if response != "y" && response != "Y" {
				cmd.Println("Cancelled")
				return
			}
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		if err := client.DeleteApp(appName); err != nil {
			slog.Error("Failed to delete app", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully deleted app '%s'", appName)))
	},
}

func init() {
	// Create app flags
	createAppCmd.Flags().String("description", "", "App description")
	createAppCmd.Flags().String("visibility", "private", "App visibility (public or private)")

	// Update app flags
	updateAppCmd.Flags().String("description", "", "App description")
	updateAppCmd.Flags().String("visibility", "", "App visibility (public or private)")
	updateAppCmd.Flags().String("production-branch", "", "Production branch")
	updateAppCmd.Flags().String("image-tag", "", "Spice.ai runtime image tag")
	updateAppCmd.Flags().Int("replicas", 0, "Number of replicas (1-10)")
	updateAppCmd.Flags().String("region", "", "Deployment region")

	// Delete app flags
	deleteAppCmd.Flags().BoolP("force", "f", false, "Skip confirmation prompt")
}
