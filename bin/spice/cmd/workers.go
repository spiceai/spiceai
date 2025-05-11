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

var workersCmd = &cobra.Command{
	Use:   "workers",
	Short: "Lists workers loaded by the Spice runtime",
	Example: `
spice workers
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		workers, err := api.GetDataSingle[api.WorkerResponse](rtcontext, "/v1/workers")
		if err != nil {
			slog.Error("listing spiced workers", "error", err)
		}

		table := make([]interface{}, len(workers.Data))
		for i, worker := range workers.Data {
			table[i] = worker
		}
		util.WriteTable(table)
	},
}

func init() {
	RootCmd.AddCommand(workersCmd)
}
