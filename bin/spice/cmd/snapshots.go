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
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var snapshotsCmd = &cobra.Command{
	Use:   "snapshots <dataset>",
	Short: "List acceleration snapshots for a dataset",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		snapshots, err := api.GetDatasetSnapshots(rtcontext, dataset)
		if err != nil {
			slog.Error("listing dataset snapshots", "dataset", dataset, "error", err)
			return
		}

		renderSnapshotListing(snapshots)
	},
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Manage dataset acceleration snapshots",
}

var snapshotCreateCmd = &cobra.Command{
	Use:   "create <dataset>",
	Short: "Create a new acceleration snapshot for a dataset",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		snapshots, err := api.CreateDatasetSnapshot(rtcontext, dataset)
		if err != nil {
			slog.Error("creating dataset snapshot", "dataset", dataset, "error", err)
			return
		}

		fmt.Printf("Created snapshot for dataset %s.\n\n", dataset)
		renderSnapshotListing(snapshots)
	},
}

var snapshotSetHeadCmd = &cobra.Command{
	Use:   "set-head <dataset> <snapshot-id>",
	Short: "Mark an existing snapshot as the current head for a dataset",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]
		snapshotID, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			slog.Error("invalid snapshot id", "value", args[1], "error", err)
			return
		}

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		res, err := api.SetDatasetSnapshotHead(rtcontext, dataset, snapshotID)
		if err != nil {
			slog.Error("updating dataset snapshot head", "dataset", dataset, "snapshot_id", snapshotID, "error", err)
			return
		}

		fmt.Println(res.Message)
	},
}

var snapshotDeleteCmd = &cobra.Command{
	Use:   "delete <dataset> <snapshot-id>",
	Short: "Delete a snapshot from a dataset",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		dataset := args[0]
		snapshotID, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			slog.Error("invalid snapshot id", "value", args[1], "error", err)
			return
		}

		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			return
		}

		res, err := api.DeleteDatasetSnapshot(rtcontext, dataset, snapshotID)
		if err != nil {
			slog.Error("deleting dataset snapshot", "dataset", dataset, "snapshot_id", snapshotID, "error", err)
			return
		}

		fmt.Println(res.Message)
	},
}

type snapshotRow struct {
	SnapshotID        uint64
	Timestamp         string
	URI               string
	SizeBytes         uint64
	Checksum          string
	ChecksumAlgorithm string
	Current           bool
}

func renderSnapshotListing(snapshots api.DatasetSnapshots) {
	fmt.Printf("Dataset: %s\n", snapshots.Dataset)
	fmt.Printf("Location: %s\n", snapshots.Location)
	if snapshots.CurrentSnapshotID != nil {
		fmt.Printf("Current Snapshot: %d\n", *snapshots.CurrentSnapshotID)
	}
	if snapshots.LastUpdatedMs != nil {
		fmt.Printf("Last Updated: %s\n", formatTimestamp(*snapshots.LastUpdatedMs))
	}
	fmt.Println()

	if len(snapshots.Snapshots) == 0 {
		fmt.Println("No snapshots found.")
		return
	}

	rows := make([]interface{}, len(snapshots.Snapshots))
	for i, snapshot := range snapshots.Snapshots {
		rows[i] = snapshotRow{
			SnapshotID:        snapshot.SnapshotID,
			Timestamp:         formatTimestamp(snapshot.TimestampMs),
			URI:               snapshot.URI,
			SizeBytes:         snapshot.SizeBytes,
			Checksum:          snapshot.Checksum,
			ChecksumAlgorithm: snapshot.ChecksumAlgorithm,
			Current:           snapshot.IsCurrent,
		}
	}

	util.WriteTable(rows)
}

func formatTimestamp(ms int64) string {
	if ms <= 0 {
		return ""
	}

	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

func init() {
	snapshotCmd.AddCommand(snapshotCreateCmd)
	snapshotCmd.AddCommand(snapshotSetHeadCmd)
	snapshotCmd.AddCommand(snapshotDeleteCmd)

	RootCmd.AddCommand(snapshotsCmd)
	RootCmd.AddCommand(snapshotCmd)
}
