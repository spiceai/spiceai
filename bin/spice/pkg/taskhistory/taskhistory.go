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

package taskhistory

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

func SqlRequestToTraces(rtcontext *context.RuntimeContext, sql string) ([]TaskHistory, error) {
	request, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/sql", rtcontext.HttpEndpoint()), strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("error creating SQL request: %w", err)
	}

	headers := rtcontext.GetHeaders()
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Accept", "Application/json")

	response, err := rtcontext.Client().Do(request)

	if err != nil {
		return nil, fmt.Errorf("error sending SQL request: %w", err)
	}
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return []TaskHistory{}, fmt.Errorf("error reading response from spiced: %w", err)
	}
	if len(raw) == 0 {
		return []TaskHistory{}, nil
	}

	traces := make([]TaskHistory, 0)

	if err := json.Unmarshal([]byte(raw), &traces); err != nil {
		return []TaskHistory{}, fmt.Errorf("error parsing response from spiced: %w", err)
	}
	return traces, nil
}

// TaskHistory represents a record in the `runtime.task_history` table.
type TaskHistory struct {
	TraceID             string               `json:"trace_id"`
	SpanID              string               `json:"span_id"`
	ParentSpanID        *string              `json:"parent_span_id,omitempty"`
	Task                string               `json:"task"`
	Input               string               `json:"input"`
	CapturedOutput      *string              `json:"captured_output,omitempty"`
	StartTime           TimeWithMilliSeconds `json:"start_time"`
	EndTime             TimeWithMilliSeconds `json:"end_time"`
	ExecutionDurationMs float64              `json:"execution_duration_ms"`
	ErrorMessage        *string              `json:"error_message,omitempty"`
	Labels              map[string]string    `json:"labels"`
}

// TimeWithMilliSeconds is a custom time type that can be unmarshalled from a JSON string with millisecond precision.
type TimeWithMilliSeconds time.Time

func (tMs *TimeWithMilliSeconds) UnmarshalJSON(b []byte) error {
	s := string(b)
	s = s[1 : len(s)-1]

	layout := "2006-01-02T15:04:05.999999"

	t, err := time.Parse(layout, s)
	if err != nil {
		return err
	}

	*tMs = TimeWithMilliSeconds(t)
	return nil
}

func (tMs TimeWithMilliSeconds) asTime() time.Time {
	return time.Time(tMs)
}
