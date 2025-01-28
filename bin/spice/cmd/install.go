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
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/runtime"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var installCmd = &cobra.Command{
	Use:     "install [flavor]",
	Aliases: []string{"i"},
	Short:   "Install the Spice.ai runtime",
	Args:    cobra.MaximumNArgs(1), // only one positional argument (flavor)
	Example: `
spice install
spice install ai

# See more at: https://spiceai.org/docs/
`,
	Run: func(cmd *cobra.Command, args []string) {
		slog.Info("Checking for latest Spice runtime release...")

		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			slog.Error("failed to check for latest CLI release version", "error", err)
		}

		flavor := constants.FlavorDefault
		if len(args) > 0 {
			var err error
			flavor, err = constants.ParseFlavor(args[0])
			if err != nil {
				slog.Error("Invalid command specified. Try: `spice install` or `spice install ai`")
				os.Exit(1)
			}
		}

		var installed bool
		force, err := cmd.Flags().GetBool("force")
		if err != nil {
			slog.Error("getting force flag", "error", err)
			os.Exit(1)
		}

		cpu, err := cmd.Flags().GetBool("cpu")
		if err != nil {
			slog.Error("getting CPU flag", "error", err)
			os.Exit(1)
		}

		if cpu && flavor != constants.FlavorAI {
			slog.Error("CPU flag is only allowed when installing the 'ai' flavor. Try: `spice install ai --cpu`")
			os.Exit(1)
		}

		if force {
			rtcontext := context.NewContext()
			err := rtcontext.Init()
			if err != nil {
				slog.Error("initializing runtime context", "error", err)
				os.Exit(1)
			}
			err = rtcontext.InstallOrUpgradeRuntime(flavor, !cpu)
			if err != nil {
				slog.Error("installing runtime", "error", err)
				os.Exit(1)
			}
			installed = true
		} else {
			installed, err = runtime.EnsureInstalled(flavor, true, !cpu)
			if err != nil {
				slog.Error("verifying runtime install", "error", err)
				os.Exit(1)
			}
		}

		if !installed {
			slog.Info("Spice.ai runtime already installed")
		}
	},
}

func init() {
	installCmd.Flags().BoolP("force", "f", false, "Force installation of the latest released runtime")
	installCmd.Flags().BoolP("cpu", "c", false, "Install the CPU accelerated version of the AI runtime")
	RootCmd.AddCommand(installCmd)
}
