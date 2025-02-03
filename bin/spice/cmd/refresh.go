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
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
)

const (
	refreshSqlFlag  = "refresh-sql"
	refreshModeFlag = "refresh-mode"
	maxJitterFlag   = "refresh-jitter-max"
)

type DatasetRefreshApiResponse struct {
	Message string `json:"message,omitempty"`
}

type DatasetRefreshApiRequest struct {
	RefreshSQL *string `json:"refresh_sql,omitempty"`
	Mode       *string `json:"refresh_mode,omitempty"`
	MaxJitter  *string `json:"refresh_jitter_max,omitempty"`
}

func constructRequest(sql string, mode string, max_jitter string) (*string, error) {
	r := DatasetRefreshApiRequest{}
	if sql == "" && mode == "" && max_jitter == "" {
		return nil, nil
	}
	if sql != "" {
		r.RefreshSQL = &sql
	}
	if mode != "" {
		r.Mode = &mode
	}
	if max_jitter != "" {
		r.MaxJitter = &max_jitter
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

# See more at: https://spiceai.org/docs/
`,
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]

		sql, _ := cmd.Flags().GetString(refreshSqlFlag)
		mode, _ := cmd.Flags().GetString(refreshModeFlag)
		maxJitter, _ := cmd.Flags().GetString(maxJitterFlag)

		// If the mode is not empty, it must be either 'full' or 'append'.
		if mode != "" && mode != spec.REFRESH_MODE_FULL && mode != spec.REFRESH_MODE_APPEND {
			slog.Error("Invalid refresh mode. Valid modes are 'full' or 'append'")
			return
		}

		slog.Info(fmt.Sprintf("Refreshing dataset %s ...\n", dataset))

		rtcontext := context.NewContext()
		if rootCertPath, err := cmd.Flags().GetString("tls-root-certificate-file"); err == nil && rootCertPath != "" {
			rtcontext = context.NewHttpsContext(rootCertPath)
		}

		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/refresh", dataset)

		body, err := constructRequest(sql, mode, maxJitter)
		if err != nil {
			slog.Error("constructing request", "error", err)
			return
		}
		res, err := api.PostRuntime[DatasetRefreshApiResponse](rtcontext, url, body)
		if err != nil {
			slog.Error("refreshing dataset", "error", err)
			return
		}

		slog.Info(res.Message)
	},
}

func init() {
	refreshCmd.Flags().String("tls-root-certificate-file", "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
	refreshCmd.Flags().String(refreshSqlFlag, "", "'refresh_sql' to refresh a dataset.")
	refreshCmd.Flags().String(refreshModeFlag, "", "'refresh_mode', one of: full, append")
	refreshCmd.Flags().String(maxJitterFlag, "", "'refresh_jitter_max', a duration string (e.g. '1m') to specify the maximum jitter allowed for the refresh operation")
	RootCmd.AddCommand(refreshCmd)
}
