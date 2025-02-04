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
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/taskhistory"
)

var (
	// The id of the trace to provide
	id string

	// The trace_id of the trace to provide
	trace_id string
)

var supported_trace_tasks = []string{"ai_chat", "ai_completion", "sql_query", "nsql",
	"tool_use::document_similarity", "tool_use::list_datasets", "tool_use::sql",
	"tool_use::table_schema", "tool_use::sample_data", "tool_use::sql_query", "tool_use::memory"}

func isValidTraceTask(task string) bool {
	for _, supported_task := range supported_trace_tasks {
		if task == supported_task {
			return true
		}
	}
	return false
}

var traceCmd = &cobra.Command{
	Use:   "trace",
	Short: "Return a user friendly trace into an operation that occurred in Spice",
	Example: `
# returns the last trace
$ spice trace ai_chat

# returns the trace for the given id
$ spice trace ai_chat --id chatcmpl-At6ZmDE8iAYRPeuQLA0FLlWxGKNnM
`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		var filter string
		var err error
		switch isValidTraceTask(args[0]) {
		case true:
			filter, err = getTraceFilter(args[0], id, trace_id)
		default:
			err = fmt.Errorf("invalid trace type %s", args[0])
		}
		if err != nil {
			cmd.Println(err)
			return
		}

		traces, err := taskhistory.SqlRequestToTraces(rtcontext, fmt.Sprintf("SELECT * FROM runtime.task_history WHERE %s ORDER BY start_time asc", filter))
		if err != nil {
			slog.Error("SQL query to 'task_history' failed", "error", err)
			cmd.PrintErrln("Error: failed to retrieve events from runtime.")
			return
		}
		if len(traces) == 0 {
			cmd.PrintErrln("Error: No events found")
			return
		}
		taskhistory.PrintTreeFromTraces(cmd.OutOrStdout(), traces, Display)
	},
}

func Display(t *taskhistory.TaskHistory) string {
	return fmt.Sprintf("(%8.2fms) %s ", t.ExecutionDurationMs, t.Task)
}

func init() {
	RootCmd.AddCommand(traceCmd)
	traceCmd.Flags().StringVar(&id, "id", "", "Return the trace with the given id")
	traceCmd.Flags().StringVar(&trace_id, "trace-id", "", "Return the trace with the given trace id")
}

func getTraceFilter(task string, id string, trace_id string) (string, error) {
	if id != "" {
		return fmt.Sprintf("trace_id=(SELECT trace_id from runtime.task_history where labels.id='%s')", id), nil
	}
	if trace_id != "" {
		return fmt.Sprintf("trace_id='%s'", trace_id), nil
	}
	// use last by default
	return fmt.Sprintf("trace_id=(SELECT trace_id from runtime.task_history where task='%s' order by start_time desc limit 1)", task), nil
}
