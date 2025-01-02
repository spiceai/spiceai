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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

type ComponentStatus int

const (
	Unknown ComponentStatus = iota
	Initializing
	Ready
	Disabled
	Error
	Refreshing
)

func (cs ComponentStatus) String() string {
	switch cs {
	case Unknown:
		return "Unknown"
	case Initializing:
		return "Initializing"
	case Ready:
		return "Ready"
	case Disabled:
		return "Disabled"
	case Error:
		return "Error"
	case Refreshing:
		return "Refreshing"
	default:
		return "Unknown"
	}
}

type ComponentStatusResult struct {
	Name          string `json:"name"`
	Status        int    `json:"status"`
	ComponentType string `json:"component_type"`
}

func GetDatasetsWithStatus(rtContext *context.RuntimeContext) ([]Dataset, error) {
	_, dataset_statuses, err := GetComponentStatuses(rtContext)
	if err != nil {
		return nil, err
	}

	datasets, err := GetData[Dataset](rtContext, "/v1/datasets?status=true")
	if err != nil {
		return nil, err
	}
	for _, dataset := range datasets {
		statusEnum, exists := dataset_statuses[dataset.Name]
		if exists {
			dataset.Status = statusEnum.String()
		}
	}
	return datasets, nil
}

// Get the status of all models and datasets (respectively).
func GetComponentStatuses(rtContext *context.RuntimeContext) (map[string]ComponentStatus, map[string]ComponentStatus, error) {
	componentStatusQuery := `
WITH combined_data AS (
    SELECT 
        array_element(attributes, 1)['str'] AS name, 
        data_number.int_value AS status, 
        'models' AS component_type, 
        time_unix_nano
    FROM runtime.metrics 
    WHERE name = 'models_status'
    
    UNION ALL
    
    SELECT 
        array_element(attributes, 1)['str'] AS name, 
        data_number.int_value AS status, 
        'datasets' AS component_type, 
        time_unix_nano
    FROM runtime.metrics 
    WHERE name = 'datasets_status'
),
ranked_data AS (
    SELECT
        name,
        status,
        component_type,
        ROW_NUMBER() OVER (PARTITION BY name, component_type ORDER BY time_unix_nano DESC) AS rn
    FROM
        combined_data
)
SELECT
    name,
    status,
    component_type
FROM
    ranked_data
WHERE
    rn = 1 AND name NOT LIKE 'runtime.%';
`
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/sql", rtContext.HttpEndpoint()), bytes.NewReader([]byte(componentStatusQuery)))
	if err != nil {
		return nil, nil, err
	}
	resp, err := rtContext.Client().Do(req)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {

		// If 400, it could be that metric service is disabled and `runtime.metrics` table doesn't exist. This isn't an error.
		if resp.StatusCode == http.StatusBadRequest {
			disabled, err := IsMetricsDisabled(rtContext)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get service status: %v", err)
			}
			if disabled {
				return nil, nil, nil
			}
		}
		return nil, nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	// Parse the JSON response
	var componentStatuses []ComponentStatusResult
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&componentStatuses)
	if err == io.EOF {
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, fmt.Errorf("failed to decode JSON response: %v", err)
	}
	datasetsStatusMap := make(map[string]ComponentStatus)
	modelStatusMap := make(map[string]ComponentStatus)
	for _, status := range componentStatuses {
		switch status.ComponentType {
		case "models":
			modelStatusMap[status.Name] = ComponentStatus(status.Status)
		case "datasets":
			datasetsStatusMap[status.Name] = ComponentStatus(status.Status)
		}
	}
	return modelStatusMap, datasetsStatusMap, nil
}

// Returns true iff the metrics service and endpoints on the spiced instance are disabled.
func IsMetricsDisabled(rtContext *context.RuntimeContext) (bool, error) {
	s, err := GetData[Service](rtContext, "/v1/status")
	if err != nil {
		return false, fmt.Errorf("failed to get service status: %v", err)
	}
	for _, service := range s {
		if service.Name == "metrics" {
			if service.Status == "Disabled" {
				return true, nil
			}
		}
	}
	return false, nil
}
