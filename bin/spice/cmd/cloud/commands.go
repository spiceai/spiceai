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
	"github.com/spf13/cobra"
)

// CreateCmd is the parent command for creating cloud resources
var CreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new Spice Cloud resource",
	Long: `Create a new Spice Cloud resource such as an app or deployment.

Examples:
  spice cloud create app my-app
  spice cloud create deployment --app 123`,
}

// DeleteCmd is the parent command for deleting cloud resources
var DeleteCmd = &cobra.Command{
	Use:     "delete",
	Aliases: []string{"rm", "remove"},
	Short:   "Delete a Spice Cloud resource",
	Long: `Delete a Spice Cloud resource such as an app.

Examples:
  spice cloud delete app 123`,
}

// GetCmd is the parent command for getting cloud resource details
var GetCmd = &cobra.Command{
	Use:   "get",
	Short: "Get details of a Spice Cloud resource",
	Long: `Get details of a Spice Cloud resource such as an app.

Examples:
  spice cloud get app 123`,
}

// UpdateCmd is the parent command for updating cloud resources
var UpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Update a Spice Cloud resource",
	Long: `Update a Spice Cloud resource such as an app.

Examples:
  spice cloud update app 123 --replicas 2`,
}

func init() {
	// Add subcommands to create
	CreateCmd.AddCommand(createAppCmd)
	CreateCmd.AddCommand(createDeploymentCmd)

	// Add subcommands to delete
	DeleteCmd.AddCommand(deleteAppCmd)

	// Add subcommands to get
	GetCmd.AddCommand(getAppCmd)

	// Add subcommands to update
	UpdateCmd.AddCommand(updateAppCmd)
}
