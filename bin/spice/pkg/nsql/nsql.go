/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

// Package nsql provides utilities for interacting with the Spice NSQL (Text-to-SQL) API.
package nsql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

// Request represents a request to the /v1/nsql endpoint.
type Request struct {
	// Query is the natural language question to convert to SQL.
	Query string `json:"query"`
	// Model is the identifier of the LLM model to use for text-to-SQL.
	Model string `json:"model"`
}

// SendRequest sends a text-to-SQL request to the /v1/nsql endpoint.
// Returns the raw HTTP response for flexible handling by callers.
func SendRequest(rtcontext *context.RuntimeContext, req *Request) (*http.Response, error) {
	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling nsql request body: %w", err)
	}

	response, err := rtcontext.DoLongRunning(
		"POST", "/v1/nsql",
		bytes.NewReader(jsonBody),
		"Content-Type", "application/json",
		"Accept", "text/plain",
	)
	if err != nil {
		return nil, fmt.Errorf("error sending nsql request: %w", err)
	}

	return response, nil
}

// SendRequestWithTrace sends a text-to-SQL request with a traceparent header
// for trace correlation. Accepts the response as SQL and returns the generated SQL string.
func SendRequestWithTrace(rtcontext *context.RuntimeContext, req *Request, traceparent string) (string, error) {
	jsonBody, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("error marshaling nsql request body: %w", err)
	}

	response, err := rtcontext.DoLongRunning(
		"POST", "/v1/nsql",
		bytes.NewReader(jsonBody),
		"Content-Type", "application/json",
		"Accept", "application/sql",
		"traceparent", traceparent,
	)
	if err != nil {
		return "", fmt.Errorf("error sending nsql request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("error reading response body: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("nsql request failed with status %d: %s", response.StatusCode, string(raw))
	}

	return strings.TrimSpace(string(raw)), nil
}
