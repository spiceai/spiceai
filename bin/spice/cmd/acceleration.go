/*
Copyright 2025 The Spice.ai OSS Authors

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
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var accelerationCmd = &cobra.Command{
	Use:   "acceleration",
	Short: "Manage dataset acceleration features",
	Long: `Commands for managing accelerated datasets including snapshots.

Use subcommands to list snapshots, view snapshot details, and perform
rollback operations.`,
	Example: `
# List all snapshots for a dataset
spice acceleration snapshots taxi_trips

# Get details of a specific snapshot
spice acceleration snapshot taxi_trips 3

# Set the current snapshot for rollback
spice acceleration set-snapshot taxi_trips 2
`,
}

var snapshotsCmd = &cobra.Command{
	Use:   "snapshots <dataset>",
	Short: "List all acceleration snapshots for a dataset",
	Long: `Lists all available snapshots for an accelerated dataset.

Shows snapshot IDs, timestamps, sizes, checksums, and indicates which
snapshot is currently set as the active one for bootstrapping.`,
	Args: cobra.ExactArgs(1),
	Example: `
# List snapshots for the taxi_trips dataset
spice acceleration snapshots taxi_trips
`,
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		if rtcontext.IsCloud() {
			slog.Error("`spice acceleration snapshots` does not support `--cloud`.")
			return
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/snapshots", dataset)
		summary, err := api.GetDataSingle[api.SnapshotSummary](rtcontext, url)
		if err != nil {
			slog.Error("failed to list snapshots", "error", err)
			return
		}

		if len(summary.Snapshots) == 0 {
			fmt.Printf("No snapshots found for dataset %s\n", dataset)
			fmt.Printf("Location: %s\n", summary.Location)
			return
		}

		fmt.Printf("Dataset: %s\n", summary.DatasetName)
		fmt.Printf("Location: %s\n", summary.Location)
		if summary.CurrentSnapshotID != nil {
			fmt.Printf("Current Snapshot ID: %d\n", *summary.CurrentSnapshotID)
		}
		lastUpdated := time.UnixMilli(summary.LastUpdatedMs).UTC().Format(time.RFC3339)
		fmt.Printf("Last Updated: %s\n", lastUpdated)
		fmt.Println()

		// Convert snapshots to table format
		table := make([]interface{}, len(summary.Snapshots))
		for i, snapshot := range summary.Snapshots {
			table[i] = snapshotTableRow{
				ID:        snapshot.SnapshotID,
				Timestamp: time.UnixMilli(snapshot.TimestampMs).UTC().Format(time.RFC3339),
				Size:      humanize.IBytes(snapshot.SizeBytes),
				Rows:      formatRowCount(snapshot.RowCount),
				Checksum:  truncateChecksum(snapshot.Checksum),
				Current:   formatBool(snapshot.IsCurrent),
			}
		}

		util.WriteTable(table)
	},
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot <dataset> <snapshot_id>",
	Short: "Get details of a specific acceleration snapshot",
	Long:  `Displays detailed information about a specific snapshot including its full location and checksum.`,
	Args:  cobra.ExactArgs(2),
	Example: `
# Get details of snapshot 3 for the taxi_trips dataset
spice acceleration snapshot taxi_trips 3
`,
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]
		snapshotIDStr := args[1]

		snapshotID, err := strconv.ParseUint(snapshotIDStr, 10, 64)
		if err != nil {
			slog.Error("invalid snapshot ID", "error", err)
			return
		}

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		if rtcontext.IsCloud() {
			slog.Error("`spice acceleration snapshot` does not support `--cloud`.")
			return
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/snapshots/%d", dataset, snapshotID)
		snapshot, err := api.GetDataSingle[api.SnapshotInfo](rtcontext, url)
		if err != nil {
			slog.Error("failed to get snapshot", "error", err)
			return
		}

		fmt.Printf("Snapshot ID: %d\n", snapshot.SnapshotID)
		fmt.Printf("Timestamp: %s\n", time.UnixMilli(snapshot.TimestampMs).UTC().Format(time.RFC3339))
		fmt.Printf("Location: %s\n", snapshot.Location)
		fmt.Printf("Size: %s (%d bytes)\n", humanize.IBytes(snapshot.SizeBytes), snapshot.SizeBytes)
		if snapshot.RowCount != nil {
			fmt.Printf("Rows: %s\n", humanize.Comma(int64(*snapshot.RowCount)))
		}
		fmt.Printf("Checksum (%s): %s\n", snapshot.ChecksumAlgorithm, snapshot.Checksum)
		fmt.Printf("Is Current: %v\n", snapshot.IsCurrent)
	},
}

var setSnapshotCmd = &cobra.Command{
	Use:   "set-snapshot <dataset> <snapshot_id>",
	Short: "Set the current snapshot for rollback",
	Long: `Sets the current snapshot pointer for an accelerated dataset.

This operation updates the metadata to point to the specified snapshot.
The next time the runtime starts, it will bootstrap from this snapshot
instead of the latest one.

WARNING: The runtime must be restarted for the rollback to take effect.
This operation only updates the metadata pointer.`,
	Args: cobra.ExactArgs(2),
	Example: `
# Set snapshot 2 as the current snapshot for taxi_trips
spice acceleration set-snapshot taxi_trips 2
`,
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]
		snapshotIDStr := args[1]

		snapshotID, err := strconv.ParseUint(snapshotIDStr, 10, 64)
		if err != nil {
			slog.Error("invalid snapshot ID", "error", err)
			return
		}

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		if rtcontext.IsCloud() {
			slog.Error("`spice acceleration set-snapshot` does not support `--cloud`.")
			return
		}

		url := fmt.Sprintf("/v1/datasets/%s/acceleration/snapshots/current", dataset)
		request := api.SetCurrentSnapshotRequest{SnapshotID: snapshotID}
		body, err := json.Marshal(request)
		if err != nil {
			slog.Error("failed to marshal request", "error", err)
			return
		}
		bodyStr := string(body)

		response, err := api.PostRuntime[api.MessageResponse](rtcontext, url, &bodyStr)
		if err != nil {
			slog.Error("failed to set current snapshot", "error", err)
			return
		}

		slog.Info(response.Message)
	},
}

// snapshotTableRow is used for tabular display of snapshots
type snapshotTableRow struct {
	ID        uint64 `csv:"ID"`
	Timestamp string `csv:"Timestamp"`
	Size      string `csv:"Size"`
	Rows      string `csv:"Rows"`
	Checksum  string `csv:"Checksum"`
	Current   string `csv:"Current"`
}

func truncateChecksum(checksum string) string {
	if len(checksum) > 16 {
		return checksum[:16] + "..."
	}
	return checksum
}

func formatBool(b bool) string {
	if b {
		return "✓"
	}
	return ""
}

func formatRowCount(rowCount *uint64) string {
	if rowCount == nil {
		return "-"
	}
	return humanize.Comma(int64(*rowCount))
}

func init() {
	accelerationCmd.AddCommand(snapshotsCmd)
	accelerationCmd.AddCommand(snapshotCmd)
	accelerationCmd.AddCommand(setSnapshotCmd)
	RootCmd.AddCommand(accelerationCmd)
}
