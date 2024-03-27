/*
 Copyright 2024 Spice AI, Inc.

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
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Spice runtime status",
	Example: `
spice status
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		err := api.WriteDataTable(rtcontext, "/v1/status", api.Service{})
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)
}
