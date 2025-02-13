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
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var (
	// The id of the trace to provide
	id string

	// The trace_id of the trace to provide
	trace_id string

	// The include input flag
	include_input bool

	// The include output flag
	include_output bool

	// The truncation length
	truncateLength int
)

var supported_trace_tasks = []string{
	"ai_chat", "accelerated_refresh", "ai_completion", "sql_query", "nsql",
	"tool_use::document_similarity", "tool_use::list_datasets", "tool_use::sql",
	"tool_use::table_schema", "tool_use::sample_data", "tool_use::sql_query", "tool_use::memory",
	"vector_search",
}

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

		rows := taskhistory.TreeRowsFromTraces(traces)

		table := make([]interface{}, len(rows))
		for i, dataset := range rows {
			table[i] = ToRowInterface(dataset.Tree, &dataset.Task, include_input, include_output, truncateLength)
		}

		util.WriteTable(table)
	},
}

// Reduce the `taskhistory.TaskHistory` to only the columns that are needed for the table. This includes the
// `treePrefix` as the first column.
//
// Must use a struct because `util.WriteTable` uses `reflect` functions that require a struct.
// Must use separate structs for each combination of input/output. Otherwise table will have columns with all `nil`s. A
// `json:"fieldName,omitempty"` tag does not work.
func ToRowInterface(treePrefix string, t *taskhistory.TaskHistory, includeInput bool, includeOutput bool, truncateLength int) interface{} {
	type TaskRowBase struct {
		Tree     string `json:"tree"`
		Status   string `json:"status"`
		Duration string `json:"duration"`
		Task     string `json:"task"`
	}
	type TaskRowFull struct {
		TaskRowBase
		Input  interface{} `json:"input"`
		Output interface{} `json:"output"`
	}
	type TaskRowWithInput struct {
		TaskRowBase
		Input interface{} `json:"input"`
	}
	type TaskRowWithOutput struct {
		TaskRowBase
		Output interface{} `json:"output"`
	}

	base := TaskRowBase{
		Tree:     treePrefix,
		Duration: fmt.Sprintf("%8.2fms", t.ExecutionDurationMs),
		Task:     t.Task,
	}

	if t.ErrorMessage == nil || *t.ErrorMessage == "" {
		base.Status = "✅"
	} else {
		base.Status = "🚫"
	}

	if includeInput {
		if len(t.Input) == 0 {
			t.Input = "<empty>"
		} else if truncateLength > 0 && len(t.Input) > truncateLength {
			originalLength := len(t.Input)
			t.Input = t.Input[:truncateLength] + "... " + fmt.Sprintf("(%d characters omitted)", originalLength-truncateLength)
		}
	}

	var output string
	if t.CapturedOutput != nil {
		if len(*t.CapturedOutput) == 0 {
			output = "<empty>"
		} else if truncateLength > 0 && len(*t.CapturedOutput) > truncateLength {
			originalLength := len(*t.CapturedOutput)
			output = (*t.CapturedOutput)[:truncateLength] + "... " + fmt.Sprintf("(%d characters omitted)", originalLength-truncateLength)
		} else {
			output = *t.CapturedOutput
		}
	} else {
		output = "<empty>"
	}

	if includeInput && includeOutput {
		return TaskRowFull{TaskRowBase: base, Input: t.Input, Output: output}
	} else if includeInput {
		return TaskRowWithInput{TaskRowBase: base, Input: t.Input}
	} else if includeOutput {
		return TaskRowWithOutput{TaskRowBase: base, Output: output}
	}
	return base
}

func init() {
	RootCmd.AddCommand(traceCmd)
	traceCmd.Flags().StringVar(&id, "id", "", "Return the trace with the given id")
	traceCmd.Flags().StringVar(&trace_id, "trace-id", "", "Return the trace with the given trace id")
	traceCmd.Flags().BoolVar(&include_input, "include-input", false, "Include input data in the trace")
	traceCmd.Flags().BoolVar(&include_output, "include-output", false, "Include output data in the trace")
	traceCmd.Flags().IntVar(&truncateLength, "truncate", 0, "Truncates the input/output data to 80 when set, or to the given length")
	traceCmd.Flags().Lookup("truncate").NoOptDefVal = "80"
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
