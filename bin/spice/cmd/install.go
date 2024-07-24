/*
Copyright 2024 The Spice.ai OSS Authors

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
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/runtime"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Install the Spice.ai runtime",
	Example: `
spice install

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
		}

		installed, err := runtime.EnsureInstalled()
		if err != nil {
			cmd.PrintErrln(err.Error())
			os.Exit(1)
		}

		if !installed {
			cmd.Println("Spice.ai runtime already installed")
		}
	},
}

func init() {
	installCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(installCmd)
}
