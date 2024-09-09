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
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
)

const (
	refreshSqlFlag  = "refresh-sql"
	refreshModeFlag = "refresh-mode"
)

type DatasetRefreshApiResponse struct {
	Message string `json:"message,omitempty"`
}

type DatasetRefreshApiRequest struct {
	RefreshSQL  *string `json:"refresh_sql,omitempty"`
	Mode *string `json:"refresh_mode,omitempty"`
}

func constructRequest(sql string, mode string) (*string, error) {
	r := DatasetRefreshApiRequest{}
	if sql == "" && mode == "" {
		return nil, nil
	}
	if sql != "" {
		r.SQL = &sql
	}
	if mode != "" {
		r.Mode = &mode
	}
	bytz, err := json.Marshal(r)
	s := string(bytz)

	return &s, err
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

		sql, _ := cmd.Flags().GetString(refreshSqlFlag)
		mode, _ := cmd.Flags().GetString(refreshModeFlag)

		// If the mode is not empty, it must be either 'full' or 'append'.
		if mode != "" && mode != spec.REFRESH_MODE_FULL && mode != spec.REFRESH_MODE_APPEND {
			cmd.PrintErrln("Invalid refresh mode. Must be 'full' or 'append'")
			return
		}

		cmd.Printf("Refreshing dataset %s ...\n", dataset)

		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-root-certificate-file"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/refresh", dataset)

		body, err := construct_request(sql, mode)
		if err != nil {
			cmd.PrintErrln(err.Error())
			return
		}
		res, err := api.PostRuntime[DatasetRefreshApiResponse](rtcontext, url, body)
		if err != nil {
			cmd.PrintErrln(err.Error())
			return
		}

		cmd.Println(res.Message)
	},
}

func init() {
	refreshCmd.Flags().BoolP("help", "h", false, "Print this help message")
	refreshCmd.Flags().String("tls-root-certificate-file", "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
	refreshCmd.Flags().String(refreshSqlFlag, "", "'refresh_sql' to refresh a dataset.")
	refreshCmd.Flags().String(refreshModeFlag, "", "'refresh_mode', one of: full, append")
	RootCmd.AddCommand(refreshCmd)
}
