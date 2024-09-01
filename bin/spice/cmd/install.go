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
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/runtime"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var installCmd = &cobra.Command{
	Use:     "install [flavor]",
	Aliases: []string{"i"},
	Short:   "Install the Spice.ai runtime",
	Example: `
spice install
spice install ai

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Checking for latest Spice runtime release...")

		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
		}

		flavor := ""
		if len(args) > 0 {
			flavor = args[0]
		}

		var installed bool
		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			cmd.PrintErrln(err.Error())
			os.Exit(1)
		}

		if force {
			rtcontext := context.NewContext()
			err := rtcontext.Init()
			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}
			err = rtcontext.InstallOrUpgradeRuntime(flavor)
			if err != nil {
				cmd.PrintErrln(err.Error())
				os.Exit(1)
			}
			installed = true
		} else {
			installed, err = runtime.EnsureInstalled(flavor)
			if err != nil {
				cmd.PrintErrln(err.Error())
				os.Exit(1)
			}
		}

		if !installed {
			cmd.Println("Spice.ai runtime already installed")
		}
	},
}

func init() {
	installCmd.Flags().BoolP("help", "h", false, "Print this help message")
	installCmd.Flags().BoolP("force", "f", false, "Force installation of the latest released runtime")
	RootCmd.AddCommand(installCmd)
}
