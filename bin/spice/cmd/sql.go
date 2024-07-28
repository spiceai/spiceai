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

		spiceArgs := []string{"--repl"}

		if rootCertPath, err := cmd.Flags().GetString("tls-ca-certificate-path"); err == nil && rootCertPath != "" {
			args = append(args, "--tls-ca-certificate-path", rootCertPath)
		}

		args = append(spiceArgs, args...)

		execCmd, err := rtcontext.GetRunCmd(args)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		execCmd.Stderr = os.Stderr
		execCmd.Stdout = os.Stdout
		execCmd.Stdin = os.Stdin

		err = util.RunCommand(execCmd)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	sqlCmd.Flags().String("tls-ca-certificate-path", "", "The path to the CA certificate file used to verify the Spice.ai runtime server certificate")
	sqlCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(sqlCmd)
}
