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
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var traceCmd = &cobra.Command{
	Use:   "trace",
	Short: "Return a user friendly trace into an operation that occurred in Spice",
	Example: `
$ spice trace chat --id chatcmpl-At6ZmDE8iAYRPeuQLA0FLlWxGKNnM

$ spice trace chat --last
`,
	Args: cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
			cmd.Print("API key set %s", apiKey)
		}
		cmd.Print(args)
	},
}

func init() {
	RootCmd.AddCommand(traceCmd)
}

//  select * from runtime.task_history where trace_id=(select trace_id from runtime.task_history where labels.id='chatcmpl-At6XgMxYOI7KB9oeJJCUu4UINbX9F')
// select * from runtime.task_history where trace_id=(select trace_id from runtime.task_history where task='ai_chat' order by start_time desc limit 1);
