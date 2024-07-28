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
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var catalogsCmd = &cobra.Command{
	Use:   "catalogs",
	Short: "Lists catalogs configured by the Spice runtime",
	Example: `
spice catalogs
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-ca-certificate-path"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}

		catalogs, err := api.GetData[api.Catalog](rtcontext, "/v1/catalogs")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
		table := make([]interface{}, len(catalogs))
		for i, catalog := range catalogs {
			table[i] = catalog
		}
		util.WriteTable(table)
	},
}

func init() {
	catalogsCmd.Flags().String("tls-ca-certificate-path", "", "The path to the CA certificate file used to verify the Spice.ai runtime server certificate")
	RootCmd.AddCommand(catalogsCmd)
}
