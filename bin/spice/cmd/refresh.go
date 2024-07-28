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

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

type DatasetRefreshApiResponse struct {
	Message string `json:"message,omitempty"`
}

var refreshCmd = &cobra.Command{
	Use:   "refresh",
	Short: "Refresh a dataset",
	Args:  cobra.MinimumNArgs(1),
	Example: `
spice refresh taxi_trips

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]

		cmd.Printf("Refreshing dataset %s ...\n", dataset)

		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-ca-certificate-path"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/refresh", dataset)
		res, err := api.PostRuntime[DatasetRefreshApiResponse](rtcontext, url)
		if err != nil {
			cmd.PrintErrln(err.Error())
			return
		}

		cmd.Println(res.Message)
	},
}

func init() {
	refreshCmd.Flags().BoolP("help", "h", false, "Print this help message")
	refreshCmd.Flags().String("tls-ca-certificate-path", "", "The path to the CA certificate file used to verify the Spice.ai runtime server certificate")
	RootCmd.AddCommand(refreshCmd)
}
