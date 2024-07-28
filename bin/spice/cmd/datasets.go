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

var datasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "Lists datasets loaded by the Spice runtime",
	Example: `
spice datasets
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-root-certificate-file"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}
		_, dataset_statuses, err := api.GetComponentStatuses(rtcontext)
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		datasets, err := api.GetData[api.Dataset](rtcontext, "/v1/datasets?status=true")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
		table := make([]interface{}, len(datasets))
		for i, dataset := range datasets {
			statusEnum, exists := dataset_statuses[dataset.Name]
			if exists {
				dataset.Status = statusEnum.String()
			}
			table[i] = dataset
		}
		util.WriteTable(table)
	},
}

func init() {
	datasetsCmd.Flags().String("tls-root-certificate-file", "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
	RootCmd.AddCommand(datasetsCmd)
}
