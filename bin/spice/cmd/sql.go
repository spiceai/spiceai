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
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var sqlCmd = &cobra.Command{
	Use:   "sql",
	Short: "Start an interactive SQL query session against the Spice.ai runtime",
	Example: `
$ spice sql
Welcome to the Spice.ai SQL REPL! Type 'help' for help.

show tables;  -- list available tables
sql> show tables
+---------------+--------------------+---------------+------------+
| table_catalog | table_schema       | table_name    | table_type |
+---------------+--------------------+---------------+------------+
| datafusion    | public             | tmp_view_test | VIEW       |
| datafusion    | information_schema | tables        | VIEW       |
| datafusion    | information_schema | views         | VIEW       |
| datafusion    | information_schema | columns       | VIEW       |
| datafusion    | information_schema | df_settings   | VIEW       |
+---------------+--------------------+---------------+------------+
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()

		_, err := rtcontext.Version()
		if err != nil {
			slog.Error("Failed to run `spice sql`: The Spice runtime is not installed. Run `spice install` and retry.")
			return
		}

		spiceArgs := []string{"--repl"}

		if rootCertPath, err := cmd.Flags().GetString("tls-root-certificate-file"); err == nil && rootCertPath != "" {
			args = append(args, "--tls-root-certificate-file", rootCertPath)
		}

		if apiKey, err := cmd.Flags().GetString("api-key"); err == nil && apiKey != "" {
			args = append(args, "--api-key", apiKey)
		}

		if userAgent, err := cmd.Flags().GetString("user-agent"); err == nil && userAgent != "" {
			args = append(args, "--user-agent", userAgent)
		}

		if cacheControl, err := cmd.Flags().GetString("cache-control"); err == nil && cacheControl != "" {
			args = append(args, "--cache-control", cacheControl)
		}

		args = append(spiceArgs, args...)

		execCmd, err := rtcontext.GetRunCmd(args)
		if err != nil {
			slog.Error("getting run command", "error", err)
			os.Exit(1)
		}

		execCmd.Stderr = os.Stderr
		execCmd.Stdout = os.Stdout
		execCmd.Stdin = os.Stdin

		err = util.RunCommand(execCmd)
		if err != nil {
			slog.Error("running command", "error", err, "command", execCmd.String())
			os.Exit(1)
		}
	},
}

func init() {
	sqlCmd.Flags().String("tls-root-certificate-file", "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
	sqlCmd.Flags().String("user-agent", "", "The user agent to use for all requests")
	sqlCmd.Flags().String("cache-control", "cache", "Control whether the results cache is used for queries. [possible values: cache, no-cache]")
	RootCmd.AddCommand(sqlCmd)
}
