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

var modelsCmd = &cobra.Command{
	Use:   "models",
	Short: "Lists models loaded by the Spice runtime",
	Example: `
spice models
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-ca-certificate-path"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}
		model_statuses, _, err := api.GetComponentStatuses(rtcontext)
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		models, err := api.GetData[api.Model](rtcontext, "/v1/models?status=true")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		table := make([]interface{}, len(models))
		for i, model := range models {
			statusEnum, exists := model_statuses[model.Name]
			if exists {
				model.Status = statusEnum.String()
			}
			table[i] = model
		}
		util.WriteTable(table)
	},
}

func init() {
	modelsCmd.Flags().String("tls-ca-certificate-path", "", "The path to the CA certificate file used to verify the Spice.ai runtime server certificate")
	RootCmd.AddCommand(modelsCmd)
}
