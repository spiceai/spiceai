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
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

// LinkCmd links the current directory to a Spice Cloud app
var LinkCmd = &cobra.Command{
	Use:   "link [org/app]",
	Short: "Link current directory to a Spice Cloud app",
	Long: `Link the current directory to a Spice Cloud app.

Once linked, commands like 'deploy', 'deployments', and 'api-keys' will 
automatically use the linked app without requiring the --app flag.

You can specify the app directly as an argument, or run interactively 
to select from your available apps.`,
	Example: `
# Link interactively (shows list of apps to choose from)
spice cloud link

# Link to a specific app
spice cloud link myorg/myapp

# Check current link status
spice cloud link --status
`,
	Run: func(cmd *cobra.Command, args []string) {
		showStatus, _ := cmd.Flags().GetBool("status")
		if showStatus {
			showLinkStatus(cmd)
			return
		}

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Not logged in. Run 'spice cloud login' first.", "error", err)
			os.Exit(1)
		}

		var appName string

		if len(args) > 0 {
			// App name provided as argument
			appName = args[0]
		} else {
			// Interactive selection
			appName = selectAppInteractively(cmd, client)
			if appName == "" {
				return // User cancelled
			}
		}

		// Validate the app exists
		app, err := client.GetApp(appName)
		if err != nil {
			slog.Error("Failed to find app", "app", appName, "error", err)
			os.Exit(1)
		}

		// Save the link
		link := &CloudLink{
			Org:      app.Org,
			App:      app.Name,
			AppID:    app.ID,
			Region:   app.Region,
			LinkedAt: time.Now().UTC().Format(time.RFC3339),
		}

		if err := SaveCloudLink(link); err != nil {
			slog.Error("Failed to save link configuration", "error", err)
			os.Exit(1)
		}

		cwd, _ := os.Getwd()
		cmd.Println()
		cmd.Println(aurora.BrightGreen(fmt.Sprintf("✓ Linked %s to %s", cwd, link.FullName())))
		cmd.Println()
		cmd.Printf("Configuration saved to %s\n", GetCloudConfigPath())
		cmd.Println()
		cmd.Println("You can now run commands without specifying --app:")
		cmd.Println("  spice cloud deploy")
		cmd.Println("  spice cloud deployments")
		cmd.Println("  spice cloud api-keys")
	},
}

// UnlinkCmd removes the link to a Spice Cloud app
var UnlinkCmd = &cobra.Command{
	Use:   "unlink",
	Short: "Unlink current directory from Spice Cloud app",
	Long:  `Remove the link between the current directory and a Spice Cloud app.`,
	Example: `
spice cloud unlink
`,
	Run: func(cmd *cobra.Command, args []string) {
		link, err := LoadCloudLink()
		if err != nil {
			slog.Error("Failed to read link configuration", "error", err)
			os.Exit(1)
		}

		if link == nil {
			cmd.Println("No app is currently linked to this directory.")
			return
		}

		appName := link.FullName()

		if err := RemoveCloudLink(); err != nil {
			slog.Error("Failed to remove link", "error", err)
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("✓ Unlinked from %s", appName)))
	},
}

func showLinkStatus(cmd *cobra.Command) {
	link, err := LoadCloudLink()
	if err != nil {
		slog.Error("Failed to read link configuration", "error", err)
		os.Exit(1)
	}

	if link == nil {
		cmd.Println("No app is currently linked to this directory.")
		cmd.Println()
		cmd.Println("Run 'spice cloud link' to link an app.")
		return
	}

	cwd, _ := os.Getwd()
	cmd.Printf("Current directory: %s\n", cwd)
	cmd.Printf("Linked app:        %s\n", link.FullName())
	if link.Region != "" {
		cmd.Printf("Region:            %s\n", link.Region)
	}
	if link.LinkedAt != "" {
		cmd.Printf("Linked at:         %s\n", link.LinkedAt)
	}
}

func selectAppInteractively(cmd *cobra.Command, client *api.CloudClient) string {
	apps, err := client.ListApps()
	if err != nil {
		slog.Error("Failed to list apps", "error", err)
		os.Exit(1)
	}

	if len(apps) == 0 {
		cmd.Println("No apps found. Create one first with 'spice cloud create app <name>'")
		return ""
	}

	cmd.Println("Select an app to link:")
	cmd.Println()

	for i, app := range apps {
		cmd.Printf("  %d. %s/%s", i+1, app.Org, app.Name)
		if app.Description != "" {
			cmd.Printf(" - %s", app.Description)
		}
		cmd.Println()
	}

	cmd.Println()
	cmd.Printf("Enter number (1-%d) or 'q' to cancel: ", len(apps))

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		slog.Error("Failed to read input", "error", err)
		os.Exit(1)
	}

	input = strings.TrimSpace(input)
	if input == "q" || input == "Q" || input == "" {
		cmd.Println("Cancelled.")
		return ""
	}

	num, err := strconv.Atoi(input)
	if err != nil || num < 1 || num > len(apps) {
		slog.Error("Invalid selection")
		os.Exit(1)
	}

	selectedApp := apps[num-1]
	return selectedApp.FullName()
}

func init() {
	LinkCmd.Flags().Bool("status", false, "Show current link status")
}
