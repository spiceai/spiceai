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
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/runtime"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Spice.ai - starts the Spice.ai runtime, installing if necessary",
	Example: `
spice run

# See more at: https://spiceai.org/docs/
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			slog.Error("failed to check for latest CLI release version", "error", err)
		}

		// Pass through verbosity of `spice run`
		level := verbosity.GetLevel()
		if level > 0 {
			args = append(args, fmt.Sprintf("-%s", strings.Repeat("v", level)))
		}

		err = runtime.Run(args)
		if err != nil {
			slog.Error("error running Spice.ai", "error", err)
			os.Exit(1)
		}
	},
}

func init() {
	RootCmd.AddCommand(runCmd)
}
