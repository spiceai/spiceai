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

package cloud

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

// LogsCmd displays logs for a deployment
var LogsCmd = &cobra.Command{
	Use:     "logs",
	Aliases: []string{"log", "tail"},
	Short:   "View logs from a deployment",
	Long: `View logs from a deployment. By default shows logs from the latest deployment.

Use --follow to stream logs in real-time (similar to 'tail -f').
Use --deployment to view logs from a specific deployment.`,
	Example: `
# View logs from the latest deployment of linked app
spice cloud logs

# View logs from a specific app
spice cloud logs --app myorg/myapp

# Stream logs in real-time
spice cloud logs --follow

# View logs from a specific deployment
spice cloud logs --deployment 12345

# Show last 100 lines
spice cloud logs --lines 100

# Output as JSON (for scripting)
spice cloud logs --json
`,
	Run: func(cmd *cobra.Command, args []string) {
		flagValue, _ := cmd.Flags().GetString("app")
		appName, err := RequireApp(flagValue)
		if err != nil {
			slog.Error(err.Error())
			os.Exit(1)
		}

		deploymentID, _ := cmd.Flags().GetInt64("deployment")
		lines, _ := cmd.Flags().GetInt("lines")
		follow, _ := cmd.Flags().GetBool("follow")
		jsonOutput, _ := cmd.Flags().GetBool("json")
		since, _ := cmd.Flags().GetString("since")

		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			slog.Error("Failed to initialize cloud client", "error", err)
			os.Exit(1)
		}

		// If no deployment specified, get the latest
		if deploymentID == 0 {
			latest, err := client.GetLatestDeployment(appName)
			if err != nil {
				slog.Error("Failed to get latest deployment", "error", err)
				os.Exit(1)
			}
			deploymentID = latest.ID
			if !jsonOutput {
				cmd.Printf("Showing logs from deployment %d\n\n", deploymentID)
			}
		}

		if follow {
			// Stream logs
			streamLogs(cmd, client, appName, deploymentID, jsonOutput)
		} else {
			// One-time fetch
			fetchLogs(cmd, client, appName, deploymentID, lines, since, jsonOutput)
		}
	},
}

func fetchLogs(cmd *cobra.Command, client *api.CloudClient, appName string, deploymentID int64, limit int, since string, jsonOutput bool) {
	logs, err := client.GetDeploymentLogs(appName, deploymentID, limit, since)
	if err != nil {
		slog.Error("Failed to get logs", "error", err)
		os.Exit(1)
	}

	if len(logs.Logs) == 0 {
		if !jsonOutput {
			cmd.Println("No logs found.")
		} else {
			cmd.Println("[]")
		}
		return
	}

	if jsonOutput {
		jsonBytes, err := json.MarshalIndent(logs.Logs, "", "  ")
		if err != nil {
			slog.Error("Failed to marshal logs to JSON", "error", err)
			os.Exit(1)
		}
		cmd.Println(string(jsonBytes))
	} else {
		for _, entry := range logs.Logs {
			printLogEntry(cmd, entry)
		}
	}
}

func streamLogs(cmd *cobra.Command, client *api.CloudClient, appName string, deploymentID int64, jsonOutput bool) {
	if !jsonOutput {
		cmd.Println("Streaming logs... (Ctrl+C to stop)")
	}

	lastTimestamp := ""
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Initial fetch
	logs, err := client.GetDeploymentLogs(appName, deploymentID, 50, "")
	if err != nil {
		slog.Error("Failed to get logs", "error", err)
		os.Exit(1)
	}

	for _, entry := range logs.Logs {
		if jsonOutput {
			jsonBytes, _ := json.Marshal(entry)
			cmd.Println(string(jsonBytes))
		} else {
			printLogEntry(cmd, entry)
		}
		lastTimestamp = entry.Timestamp
	}

	// Poll for new logs
	for range ticker.C {
		logs, err := client.GetDeploymentLogs(appName, deploymentID, 100, lastTimestamp)
		if err != nil {
			slog.Warn("Failed to fetch logs", "error", err)
			continue
		}

		for _, entry := range logs.Logs {
			if entry.Timestamp != lastTimestamp {
				if jsonOutput {
					jsonBytes, _ := json.Marshal(entry)
					cmd.Println(string(jsonBytes))
				} else {
					printLogEntry(cmd, entry)
				}
				lastTimestamp = entry.Timestamp
			}
		}
	}
}

func printLogEntry(cmd *cobra.Command, entry api.LogEntry) {
	timestamp := entry.Timestamp
	if t, err := time.Parse(time.RFC3339, timestamp); err == nil {
		timestamp = t.Local().Format("2006-01-02 15:04:05")
	}

	level := entry.Level
	if level == "" {
		level = "INFO"
	}

	source := ""
	if entry.Source != "" {
		source = fmt.Sprintf("[%s] ", entry.Source)
	}

	cmd.Printf("%s %s %s%s\n", timestamp, level, source, entry.Message)
}

func init() {
	LogsCmd.Flags().String("app", "", "App name in org/app format (uses linked app if not specified)")
	LogsCmd.Flags().Int64("deployment", 0, "Deployment ID (uses latest if not specified)")
	LogsCmd.Flags().Int("lines", 50, "Number of log lines to show")
	LogsCmd.Flags().BoolP("follow", "f", false, "Stream logs in real-time")
	LogsCmd.Flags().String("since", "", "Show logs since timestamp (RFC3339)")
	LogsCmd.Flags().Bool("json", false, "Output logs as JSON")
}
