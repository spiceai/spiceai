/*
Copyright 2024 The Spice.ai OSS Authors

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
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
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

const acceptHeader = `application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3`

// Get the status of all models and datasets (respectively).
func GetComponentStatuses(rtContext *context.RuntimeContext) (map[string]ComponentStatus, map[string]ComponentStatus, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/metrics", rtContext.MetricsEndpoint()), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Add("Accept", acceptHeader)
	resp, err := rtContext.Client().Do(req)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}
	metricFamilies, err := parseResponse(resp)
	if err != nil {
		return nil, nil, err
	}
	models, datasets := extractComponentStatuses(metricFamilies)
	return models, datasets, nil
}
func parseResponse(resp *http.Response) (map[string]*dto.MetricFamily, error) {
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))

	metricFamilies := make(map[string]*dto.MetricFamily, 0)
	if err == nil && mediaType == "application/vnd.google.protobuf" && params["encoding"] == "delimited" && params["proto"] == "io.prometheus.client.MetricFamily" {
		for {
			mf := &dto.MetricFamily{}
			if _, err = pbutil.ReadDelimited(resp.Body, mf); err != nil {
				if err == io.EOF {
					break
				}
			}
			metricFamilies[*mf.Name] = mf
		}
	} else {
		var parser expfmt.TextParser
		var err error
		metricFamilies, err = parser.TextToMetricFamilies(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading text format failed: %w", err)
		}
	}
	return metricFamilies, nil
}

func extractComponentStatuses(metricFamilies map[string]*dto.MetricFamily) (map[string]ComponentStatus, map[string]ComponentStatus) {
	modelStatuses := make(map[string]ComponentStatus)
	datasetStatuses := make(map[string]ComponentStatus)

	for _, mf := range metricFamilies {
		switch *mf.Name {
		case "model_status":
			for _, m := range mf.Metric {
				for _, lp := range m.Label {
					if lp.GetName() == "model" {
						modelStatuses[lp.GetValue()] = ComponentStatus(m.GetGauge().GetValue())
					}
				}
			}
		case "dataset_status":
			for _, m := range mf.Metric {
				for _, lp := range m.Label {
					if lp.GetName() == "dataset" {
						datasetStatuses[lp.GetValue()] = ComponentStatus(m.GetGauge().GetValue())
					}
				}
			}
		}
	}
	return modelStatuses, datasetStatuses
}
