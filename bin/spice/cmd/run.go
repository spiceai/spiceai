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

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Spice.ai - starts the Spice.ai runtime, installing if necessary",
	Example: `
spice run

# See more at: https://docs.spiceai.org/
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
		}

		err = runtime.Run(args)
		if err != nil {
			cmd.PrintErrln(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	runCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(runCmd)
}
