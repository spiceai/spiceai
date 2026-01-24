/*
Copyright 2026 The Spice.ai OSS Authors

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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	runtimecontext "github.com/spiceai/spiceai/bin/spice/pkg/context"
)

// QueriesClient provides methods to interact with the /v1/queries API.
type QueriesClient struct {
	rtcontext *runtimecontext.RuntimeContext
}

// NewQueriesClient creates a new client for the async queries API.
func NewQueriesClient(rtcontext *runtimecontext.RuntimeContext) *QueriesClient {
	return &QueriesClient{
		rtcontext: rtcontext,
	}
}

// SubmitRequest is the request body for submitting a new query.
type SubmitRequest struct {
	SQL            string      `json:"sql"`
	Parameters     interface{} `json:"parameters,omitempty"`
	TimeoutSeconds int         `json:"timeout_seconds,omitempty"`
}

// SubmitResponse is the response from submitting a query.
type SubmitResponse struct {
	QueryID    string      `json:"query_id"`
	Status     QueryStatus `json:"status"`
	StatusURL  string      `json:"status_url"`
	ResultsURL string      `json:"results_url"`
}

// QueryStatus represents the current status of a query.
type QueryStatus struct {
	State string      `json:"state"` // PENDING, RUNNING, SUCCEEDED, FAILED, CANCELLED, CLOSED
	Error *QueryError `json:"error,omitempty"`
}

// QueryError contains error details for a failed query.
type QueryError struct {
	ErrorCode string `json:"error_code"`
	Message   string `json:"message"`
	SQLState  string `json:"sql_state,omitempty"`
}

// QueryInfo contains full information about a query.
type QueryInfo struct {
	QueryID     string          `json:"query_id"`
	Status      QueryStatus     `json:"status"`
	Manifest    *ResultManifest `json:"manifest,omitempty"`
	Result      *ResultChunk    `json:"result,omitempty"`
	CreatedAt   string          `json:"created_at"`
	StartedAt   string          `json:"started_at,omitempty"`
	CompletedAt string          `json:"completed_at,omitempty"`
	ExpiresAt   string          `json:"expires_at,omitempty"`
}

// ResultManifest describes the result set metadata.
type ResultManifest struct {
	Format          string       `json:"format"`
	Schema          ResultSchema `json:"schema"`
	TotalRowCount   int          `json:"total_row_count"`
	TotalChunkCount int          `json:"total_chunk_count"`
	Truncated       bool         `json:"truncated"`
}

// ResultSchema describes the schema of the result set.
type ResultSchema struct {
	ColumnCount int            `json:"column_count"`
	Columns     []ColumnSchema `json:"columns"`
}

// ColumnSchema describes a single column in the result set.
type ColumnSchema struct {
	Name     string `json:"name"`
	TypeName string `json:"type_name"`
	Nullable bool   `json:"nullable"`
	Position int    `json:"position"`
}

// ResultChunk contains a chunk of result data.
type ResultChunk struct {
	ChunkIndex     int                      `json:"chunk_index"`
	RowOffset      int                      `json:"row_offset"`
	RowCount       int                      `json:"row_count"`
	NextChunkIndex *int                     `json:"next_chunk_index,omitempty"`
	NextChunkURL   string                   `json:"next_chunk_url,omitempty"`
	DataArray      []map[string]interface{} `json:"data_array,omitempty"`
}

// QueryListResponse is the response from listing queries.
type QueryListResponse struct {
	Queries    []QuerySummary `json:"queries"`
	TotalCount int            `json:"total_count"`
}

// QuerySummary is a summary of a query for listing.
type QuerySummary struct {
	QueryID    string `json:"query_id"`
	State      string `json:"state"`
	SQLPreview string `json:"sql_preview"`
	CreatedAt  string `json:"created_at"`
}

// Submit submits a new SQL query for async execution.
func (c *QueriesClient) Submit(ctx context.Context, req *SubmitRequest) (*SubmitResponse, error) {
	jsonBody, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	resp, err := c.doRequest(ctx, http.MethodPost, "/v1/queries", strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("async queries require cluster mode with scheduler.state_location configured")
	}

	if resp.StatusCode != http.StatusAccepted {
		return nil, c.parseError(resp)
	}

	var result SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetStatus gets only the status of a query.
func (c *QueriesClient) GetStatus(ctx context.Context, queryID string) (*QueryStatus, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/v1/queries/%s/status", queryID), nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("query '%s' not found", queryID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result QueryStatus
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetQuery gets full query information including first result chunk if completed.
func (c *QueriesClient) GetQuery(ctx context.Context, queryID string) (*QueryInfo, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/v1/queries/%s", queryID), nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("query '%s' not found", queryID)
	}

	if resp.StatusCode == http.StatusGone {
		return nil, fmt.Errorf("query '%s' results have expired", queryID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result QueryInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetResults gets results for a completed query.
func (c *QueriesClient) GetResults(ctx context.Context, queryID string, chunkIndex int) (*ResultChunk, error) {
	path := fmt.Sprintf("/v1/queries/%s/results", queryID)
	if chunkIndex > 0 {
		path = fmt.Sprintf("/v1/queries/%s/results/chunks/%d", queryID, chunkIndex)
	}

	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("query '%s' not found", queryID)
	}

	if resp.StatusCode == http.StatusGone {
		return nil, fmt.Errorf("query '%s' results have expired or were cancelled", queryID)
	}

	// 425 Too Early or 409 Conflict - query not complete
	if resp.StatusCode == 425 || resp.StatusCode == http.StatusConflict {
		return nil, fmt.Errorf("query '%s' is not yet complete", queryID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result ResultChunk
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// Cancel cancels a running query.
func (c *QueriesClient) Cancel(ctx context.Context, queryID string) (*QueryInfo, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/v1/queries/%s/cancel", queryID), nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("query '%s' not found", queryID)
	}

	if resp.StatusCode == http.StatusConflict {
		return nil, fmt.Errorf("query '%s' has already completed", queryID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result QueryInfo
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// List lists queries with optional status filter.
func (c *QueriesClient) List(ctx context.Context, status string, limit int) (*QueryListResponse, error) {
	path := "/v1/queries"
	params := []string{}
	if status != "" {
		params = append(params, fmt.Sprintf("status=%s", status))
	}
	if limit > 0 {
		params = append(params, fmt.Sprintf("limit=%d", limit))
	}
	if len(params) > 0 {
		path = path + "?" + strings.Join(params, "&")
	}

	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)

	if resp.StatusCode == http.StatusServiceUnavailable {
		return nil, fmt.Errorf("async queries require cluster mode with scheduler.state_location configured")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, c.parseError(resp)
	}

	var result QueryListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// doRequest performs an HTTP request with context support.
func (c *QueriesClient) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	// Create request with context
	url := fmt.Sprintf("%s%s", c.rtcontext.HttpEndpoint(), path)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	headers := c.rtcontext.GetHeaders()
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	// Use long-running client since queries can take time
	client := c.rtcontext.LongRunningClient()
	resp, err := client.Do(req)
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			return nil, c.rtcontext.RuntimeUnavailableError()
		}
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

// parseError extracts an error message from an HTTP response.
func (c *QueriesClient) parseError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("request failed with status %d", resp.StatusCode)
	}

	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("%s", errResp.Error)
	}

	return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
}

// closeBody safely closes an HTTP response body.
func closeBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		if err := resp.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
	}
}

// IsTerminal returns true if the query state is terminal (no longer running).
func (s *QueryStatus) IsTerminal() bool {
	switch strings.ToUpper(s.State) {
	case "SUCCEEDED", "FAILED", "CANCELLED", "CLOSED":
		return true
	default:
		return false
	}
}

// IsSuccess returns true if the query completed successfully.
func (s *QueryStatus) IsSuccess() bool {
	return strings.ToUpper(s.State) == "SUCCEEDED"
}

// IsFailed returns true if the query failed.
func (s *QueryStatus) IsFailed() bool {
	return strings.ToUpper(s.State) == "FAILED"
}

// IsCancelled returns true if the query was cancelled.
func (s *QueryStatus) IsCancelled() bool {
	return strings.ToUpper(s.State) == "CANCELLED"
}

// PollInterval is the default interval between status polls.
const PollInterval = 500 * time.Millisecond
