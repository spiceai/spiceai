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

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/cmd/cloud"
)

var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "Manage Spice Cloud resources",
	Long: `Manage Spice Cloud resources including apps, deployments, and API keys.

Use 'spice cloud <command>' to interact with Spice Cloud.

To get started, authenticate with 'spice cloud login'.
Then link your project to an app with 'spice cloud link'.`,
	Example: `
# Login to Spice Cloud
spice cloud login

# Check current user
spice cloud whoami

# List all apps
spice cloud apps

# Create a new app
spice cloud create app my-app

# Link current directory to an app
spice cloud link myorg/my-app

# Deploy (uses linked app)
spice cloud deploy

# View deployment logs
spice cloud logs -f

# Check deployment status
spice cloud inspect

# Manage secrets
spice cloud secrets list
spice cloud secrets set DATABASE_PASSWORD "my-secret"

# Rollback to previous deployment
spice cloud rollback

# Unlink and logout
spice cloud unlink
spice cloud logout
`,
}

func init() {
	// Add cloud subcommands
	cloudCmd.AddCommand(cloud.LoginCmd)
	cloudCmd.AddCommand(cloud.LogoutCmd)
	cloudCmd.AddCommand(cloud.WhoamiCmd)
	cloudCmd.AddCommand(cloud.LinkCmd)
	cloudCmd.AddCommand(cloud.UnlinkCmd)
	cloudCmd.AddCommand(cloud.AppsCmd)
	cloudCmd.AddCommand(cloud.CreateCmd)
	cloudCmd.AddCommand(cloud.GetCmd)
	cloudCmd.AddCommand(cloud.UpdateCmd)
	cloudCmd.AddCommand(cloud.DeploymentsCmd)
	cloudCmd.AddCommand(cloud.DeleteCmd)
	cloudCmd.AddCommand(cloud.DeployCmd)
	cloudCmd.AddCommand(cloud.APIKeysCmd)
	cloudCmd.AddCommand(cloud.RegionsCmd)
	cloudCmd.AddCommand(cloud.ImagesCmd)
	cloudCmd.AddCommand(cloud.LogsCmd)
	cloudCmd.AddCommand(cloud.SecretsCmd)
	cloudCmd.AddCommand(cloud.InspectCmd)
	cloudCmd.AddCommand(cloud.RollbackCmd)

	RootCmd.AddCommand(cloudCmd)
}
