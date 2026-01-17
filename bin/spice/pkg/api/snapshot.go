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

package api

// SnapshotInfo represents details about a single acceleration snapshot.
type SnapshotInfo struct {
	SnapshotID        uint64  `json:"snapshot_id" csv:"snapshot_id"`
	TimestampMs       int64   `json:"timestamp_ms" csv:"timestamp_ms"`
	Location          string  `json:"location" csv:"location"`
	Checksum          string  `json:"checksum" csv:"checksum"`
	ChecksumAlgorithm string  `json:"checksum_algorithm" csv:"checksum_algorithm"`
	SizeBytes         uint64  `json:"size_bytes" csv:"size_bytes"`
	RowCount          *uint64 `json:"row_count,omitempty" csv:"row_count"`
	IsCurrent         bool    `json:"is_current" csv:"is_current"`
}

// SnapshotSummary contains all snapshots for a dataset.
type SnapshotSummary struct {
	DatasetName       string         `json:"dataset_name" csv:"dataset_name"`
	Location          string         `json:"location" csv:"location"`
	LastUpdatedMs     int64          `json:"last_updated_ms" csv:"last_updated_ms"`
	CurrentSnapshotID *uint64        `json:"current_snapshot_id,omitempty" csv:"current_snapshot_id"`
	Snapshots         []SnapshotInfo `json:"snapshots" csv:"-"`
}

// SetCurrentSnapshotRequest is the request body for setting the current snapshot.
type SetCurrentSnapshotRequest struct {
	SnapshotID uint64 `json:"snapshot_id"`
}

// MessageResponse is a generic response with a message.
type MessageResponse struct {
	Message string `json:"message"`
}
