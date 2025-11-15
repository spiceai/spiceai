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

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

type DatasetSnapshots struct {
	Dataset           string            `json:"dataset"`
	Location          string            `json:"location"`
	LastUpdatedMs     *int64            `json:"last_updated_ms"`
	CurrentSnapshotID *uint64           `json:"current_snapshot_id"`
	Snapshots         []DatasetSnapshot `json:"snapshots"`
}

type DatasetSnapshot struct {
	SnapshotID        uint64 `json:"snapshot_id"`
	TimestampMs       int64  `json:"timestamp_ms"`
	URI               string `json:"uri"`
	SizeBytes         uint64 `json:"size_bytes"`
	Checksum          string `json:"checksum"`
	ChecksumAlgorithm string `json:"checksum_algorithm"`
	IsCurrent         bool   `json:"is_current"`
}

type MessageResponse struct {
	Message string `json:"message"`
}

func datasetSnapshotsBasePath(dataset string) string {
	return fmt.Sprintf("/v1/datasets/%s/acceleration/snapshots", url.PathEscape(dataset))
}

func GetDatasetSnapshots(rtcontext *context.RuntimeContext, dataset string) (DatasetSnapshots, error) {
	path := datasetSnapshotsBasePath(dataset)
	return GetDataSingle[DatasetSnapshots](rtcontext, path)
}

func CreateDatasetSnapshot(rtcontext *context.RuntimeContext, dataset string) (DatasetSnapshots, error) {
	path := datasetSnapshotsBasePath(dataset)
	return PostRuntime[DatasetSnapshots](rtcontext, path, nil)
}

func SetDatasetSnapshotHead(rtcontext *context.RuntimeContext, dataset string, snapshotID uint64) (MessageResponse, error) {
	path := fmt.Sprintf("%s/head", datasetSnapshotsBasePath(dataset))
	bodyBytes, err := json.Marshal(struct {
		SnapshotID uint64 `json:"snapshot_id"`
	}{SnapshotID: snapshotID})
	if err != nil {
		return MessageResponse{}, err
	}
	body := string(bodyBytes)
	return PatchRuntime[MessageResponse](rtcontext, path, &body)
}

func DeleteDatasetSnapshot(rtcontext *context.RuntimeContext, dataset string, snapshotID uint64) (MessageResponse, error) {
	path := fmt.Sprintf("%s/%d", datasetSnapshotsBasePath(dataset), snapshotID)
	return DeleteRuntime[MessageResponse](rtcontext, path)
}
