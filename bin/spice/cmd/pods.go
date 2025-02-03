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

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var podsCmd = &cobra.Command{
	Use:   "pods",
	Short: "Lists Spicepods loaded by the Spice runtime",
	Example: `
spice pods
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-root-certificate-file"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}

		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		spicepods, err := api.GetData[api.Spicepod](rtcontext, "/v1/spicepods")
		if err != nil {
			slog.Error("listing spiced pods", "error", err)
		}
		table := make([]interface{}, len(spicepods))
		for i, spicepod := range spicepods {
			spicepodStatus := api.SpicepodStatus{
				Version:           spicepod.Version,
				Name:              spicepod.Name,
				DatasetsCount:     len(spicepod.Datasets),
				ModelsCount:       len(spicepod.Models),
				DependenciesCount: len(spicepod.Dependencies),
			}
			table[i] = spicepodStatus
		}
		util.WriteTable(table)
	},
}

func init() {
	podsCmd.Flags().String("tls-root-certificate-file", "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
	RootCmd.AddCommand(podsCmd)
}
