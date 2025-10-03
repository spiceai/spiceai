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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/display"
	spice_http "github.com/spiceai/spiceai/bin/spice/pkg/http"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	limitKeyFlag     = "limit"
	cacheControlFlag = "cache-control"
)

type SearchRequest struct {
	Text              string   `json:"text"`
	Datasets          []string `json:"datasets,omitempty"`
	Limit             uint     `json:"limit"`
	AdditionalColumns []string `json:"additional_columns,omitempty"`
	Where             string   `json:"where,omitempty"`
}

type StringOrSlice []string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = StringOrSlice{single}
		return nil
	}

	var multiple []string
	if err := json.Unmarshal(data, &multiple); err == nil {
		*s = multiple
		return nil
	}

	return fmt.Errorf("invalid format for StringOrSlice: %s", data)
}

type SearchMatch struct {
	Matches    map[string]StringOrSlice `json:"matches"`
	Score      float64                  `json:"score"`
	Dataset    string                   `json:"dataset"`
	PrimaryKey map[string]interface{}   `json:"primary_key"`
	Metadata   map[string]interface{}   `json:"metadata"`
	Data       map[string]interface{}   `json:"data"`
}

type SearchResponse struct {
	Results    []SearchMatch `json:"results"`
	DurationMs uint64        `json:"duration_ms"`
}

var searchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search datasets with embeddings",
	Example: `
# Start a search session with local spiced instance
spice search

# Start a search session with spiced instance in spice.ai cloud
spice search --cloud
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext, err := context.FromFlags(cmd.Flags())
		if err != nil {
			slog.Error("failed to initialize runtime context", "error", err)
			os.Exit(1)
		}

		// Check for --endpoint flag for remote HTTP mode
		endpoint, err := cmd.Flags().GetString("endpoint")
		if err != nil {
			slog.Error("getting endpoint flag", "error", err)
			os.Exit(1)
		}

		// Check for --cloud flag
		if rtcontext.IsCloud() {
			if endpoint != "" {
				slog.Error("cannot use both --cloud and --endpoint flags")
				os.Exit(1)
			}

			// Get API key from context or environment variable
			apiKey := os.Getenv("SPICE_API_KEY")
			if apiKey == "" {
				if cmdApiKey, err := rtcontext.GetApiKey(); err == nil && cmdApiKey != "" {
					apiKey = cmdApiKey
				}
			}

			if apiKey == "" {
				slog.Error("API key is required when using --cloud. Set SPICE_API_KEY environment variable or use --api-key flag.")
				os.Exit(1)
			}

			// Use cloud connection - cloud uses HTTPS
			runRemoteSearchREPL(cmd, rtcontext, "https://data.spiceai.io")
			return
		}

		if endpoint != "" {
			// Remote HTTP mode
			runRemoteSearchREPL(cmd, rtcontext, endpoint)
			return
		}

		if !rtcontext.IsCloud() {
			rtcontext.RequireModelsFlavor(cmd)
		}

		datasets, err := api.GetDatasetsWithStatus(rtcontext)
		if err != nil {
			slog.Error("could not list datasets", "error", err)
		}

		for _, dataset := range datasets {
			if dataset.Status != api.Ready.String() && dataset.Status != api.Refreshing.String() {
				// warn only if search is supported by the dataset
				prop_val, _ := dataset.GetPropertyValue("search")
				if prop_val == "supported" {
					slog.Warn(fmt.Sprintf("Dataset %s is not ready (%s) and will be excluded from the search.", dataset.Name, dataset.Status))
				}
			}
		}

		matches := map[string][]SearchMatch{}

		limit, err := cmd.Flags().GetUint(limitKeyFlag)
		if err != nil {
			slog.Error("could not get limit flag", "error", err)
			os.Exit(1)
		}

		cache_control, err := cmd.Flags().GetString(cacheControlFlag)
		if err != nil {
			slog.Error("could not get cache control flag", "error", err)
			os.Exit(1)
		}

		if cache_control != "cache" && cache_control != "no-cache" {
			slog.Error("invalid value for cache-control flag. Possible values: cache, no-cache")
			os.Exit(1)
		}

		cmd.Println("Welcome to the Spice.ai search REPL! Enter your search queries.")
		cmd.Println()

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer func() {
			if err := line.Close(); err != nil {
				slog.Error("closing line", "error", err)
			}
		}()
		for {
			message, err := line.Prompt("search> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err == io.EOF {
				// EOF reached (Ctrl+D or piped input exhausted)
				break
			} else if err != nil {
				slog.Error("reading input line", "error", err)
				break
			}

			if strings.Trim(message, " ") == "" {
				cmd.Println("Enter a search query.")
				continue
			}

			line.AppendHistory(message)
			done := make(chan bool)
			go func() {
				util.ShowSpinner(done)
			}()

			responses := make(chan *http.Response)
			go func(out chan *http.Response) {
				defer close(out)
				response, err := sendSearchRequest(rtcontext, &SearchRequest{
					Text:     message,
					Datasets: nil, // search across all datasets containing embeddings
					Limit:    limit,
				}, cache_control)
				if err != nil {
					slog.Error("failed to send search request to spiced", "error", err)
					out <- nil
				} else {
					out <- response
				}
			}(responses)

			response := <-responses
			done <- true
			if response == nil {
				// Error already printed in goroutine
				continue
			}

			raw, err := io.ReadAll(response.Body)
			if err != nil {
				slog.Error("reading response from spiced", "error", err)
				continue
			}

			if response.StatusCode != 200 {
				slog.Error("search failed", "error", raw)
				continue
			}

			var searchResponse = SearchResponse{}
			err = json.Unmarshal([]byte(raw), &searchResponse)
			if err != nil {
				slog.Error("parsing response from spiced", "error", err)
				continue
			}

			// Display results in table format
			displaySearchResults(searchResponse.Results)

			matches[message] = append(matches[message], searchResponse.Results...)
			cmd.Printf("\nTime: %s. %d results.\n\n", time.Duration(searchResponse.DurationMs)*time.Millisecond, len(searchResponse.Results))
		}
	},
}

func displaySearchResults(results []SearchMatch) {
	if len(results) == 0 {
		fmt.Println("No results.")
		return
	}

	// Check if any results have primary keys and collect key names
	var primaryKeyNames []string
	for _, match := range results {
		if len(match.PrimaryKey) > 0 {
			for k := range match.PrimaryKey {
				found := false
				for _, existing := range primaryKeyNames {
					if existing == k {
						found = true
						break
					}
				}
				if !found {
					primaryKeyNames = append(primaryKeyNames, k)
				}
			}
		}
	}

	// Build table data - columns vary based on presence of primary keys
	var colNames []string
	if len(primaryKeyNames) > 0 {
		// Build key column name with all key names
		keyColName := "Key (" + strings.Join(primaryKeyNames, ", ") + ")"
		colNames = []string{"Rank", keyColName, "Match", "Score", "Dataset(s)"}
	} else {
		colNames = []string{"Rank", "Match", "Score", "Dataset(s)"}
	}
	colWidths := make([]int, len(colNames))
	for i, name := range colNames {
		colWidths[i] = len(name)
	}

	var rows [][]string
	for i, match := range results {
		rank := fmt.Sprintf("%d", i+1)
		score := fmt.Sprintf("%.4f", match.Score)
		dataset := match.Dataset

		// Format primary key value if present (just the value, not the key name)
		var primaryKey string
		if len(match.PrimaryKey) > 0 {
			var keyParts []string
			for _, keyName := range primaryKeyNames {
				if v, ok := match.PrimaryKey[keyName]; ok {
					keyParts = append(keyParts, fmt.Sprintf("%v", v))
				}
			}
			primaryKey = strings.Join(keyParts, ", ")
		}

		// Collect all match text and show first 3 lines
		var matchTexts []string
		for col, values := range match.Matches {
			for _, value := range values {
				displayValue := value

				// Show first 3 lines if multiline
				lines := strings.Split(displayValue, "\n")
				if len(lines) > 3 {
					displayValue = strings.Join(lines[:3], "\n")
				}
				displayValue = strings.ReplaceAll(displayValue, "\r", "")

				if len(match.Matches) > 1 {
					matchTexts = append(matchTexts, fmt.Sprintf("%s: %s", col, displayValue))
				} else {
					matchTexts = append(matchTexts, displayValue)
				}
			}
		}
		matchText := strings.Join(matchTexts, "; ")

		var row []string
		if len(primaryKeyNames) > 0 {
			row = []string{rank, primaryKey, matchText, score, dataset}
		} else {
			row = []string{rank, matchText, score, dataset}
		}

		// Update column widths - find max line length for multi-line cells
		for j, val := range row {
			lines := strings.Split(val, "\n")
			for _, line := range lines {
				if len(line) > colWidths[j] {
					colWidths[j] = len(line)
				}
			}
		}

		rows = append(rows, row)
	}

	// Display the table
	display.Table(colNames, nil, rows, colWidths)
}

func runRemoteSearchREPL(cmd *cobra.Command, rtcontext *context.RuntimeContext, httpEndpoint string) {
	// Get API key from context or environment variable
	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		if cmdApiKey, err := rtcontext.GetApiKey(); err == nil && cmdApiKey != "" {
			apiKey = cmdApiKey
		}
	}

	limit, err := cmd.Flags().GetUint(limitKeyFlag)
	if err != nil {
		slog.Error("could not get limit flag", "error", err)
		os.Exit(1)
	}

	cache_control, err := cmd.Flags().GetString(cacheControlFlag)
	if err != nil {
		slog.Error("could not get cache control flag", "error", err)
		os.Exit(1)
	}

	if cache_control != "cache" && cache_control != "no-cache" {
		slog.Error("invalid value for cache-control flag. Possible values: cache, no-cache")
		os.Exit(1)
	}

	// Parse custom headers
	customHeaders := make(map[string]string)
	if headers, err := cmd.Flags().GetStringSlice("headers"); err == nil {
		for _, header := range headers {
			parts := strings.SplitN(header, ":", 2)
			if len(parts) == 2 {
				customHeaders[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	// Create HTTP client
	httpClient := &http.Client{
		Timeout: 0, // No timeout for long-running queries
	}

	cmd.Println("Welcome to the Spice.ai search REPL! Enter your search queries.")
	cmd.Println()

	// Check server health and readiness
	checkDuration, healthOk := util.CheckRemoteServerHealth(httpEndpoint, httpClient, apiKey)
	if healthOk {
		cmd.Printf("Connected to %s (%dms).\n", httpEndpoint, checkDuration.Milliseconds())
	}
	cmd.Println()

	line := liner.NewLiner()
	line.SetCtrlCAborts(true)
	defer func() {
		if err := line.Close(); err != nil {
			slog.Error("closing line", "error", err)
		}
	}()

	for {
		message, err := line.Prompt("search> ")
		if err == liner.ErrPromptAborted {
			break
		} else if err == io.EOF {
			// EOF reached (Ctrl+D or piped input exhausted)
			break
		} else if err != nil {
			slog.Error("reading input line", "error", err)
			break
		}

		if strings.Trim(message, " ") == "" {
			cmd.Println("Enter a search query.")
			continue
		}

		line.AppendHistory(message)

		// Send search request
		searchReq := &SearchRequest{
			Text:     message,
			Datasets: nil,
			Limit:    limit,
		}

		jsonBody, err := json.Marshal(searchReq)
		if err != nil {
			slog.Error("marshaling search request", "error", err)
			continue
		}

		startTime := time.Now()
		req, err := http.NewRequest("POST", fmt.Sprintf("%s/v1/search", httpEndpoint), bytes.NewReader(jsonBody))
		if err != nil {
			slog.Error("creating request", "error", err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Cache-Control", cache_control)
		req.Header.Set("Accept-Encoding", "zstd, gzip, deflate")
		req.Header.Set("User-Agent", spice_http.UserAgent())
		if apiKey != "" {
			req.Header.Set("X-API-Key", apiKey)
		}

		// Add custom headers
		for key, value := range customHeaders {
			req.Header.Set(key, value)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			slog.Error("sending request", "error", err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err := resp.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
		if err != nil {
			slog.Error("reading response", "error", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			slog.Error("search failed", "error", string(body))
			continue
		}

		// Decompress response if needed
		contentEncoding := resp.Header.Get("Content-Encoding")
		if contentEncoding != "" {
			body, err = decompressResponse(body, contentEncoding)
			if err != nil {
				slog.Error("decompressing response", "error", err)
				continue
			}
		}

		var searchResponse SearchResponse
		if err := json.Unmarshal(body, &searchResponse); err != nil {
			slog.Error("parsing response", "error", err)
			continue
		}

		duration := time.Since(startTime)
		dataSize := uint64(len(body))
		transferRate := float64(dataSize) / duration.Seconds()

		// Check cache status header
		cacheStatus := resp.Header.Get("Search-Results-Cache-Status")
		cachedStr := ""
		if cacheStatus == "HIT" {
			cachedStr = " (cached)"
		}

		// Display results
		displaySearchResults(searchResponse.Results)

		cmd.Printf("\nTime: %v seconds. %d results%s. %s (%s/sec).\n\n",
			duration.Seconds(),
			len(searchResponse.Results),
			cachedStr,
			humanize.IBytes(dataSize),
			humanize.IBytes(uint64(transferRate)))
	}
}

func sendSearchRequest(rtcontext *context.RuntimeContext, body *SearchRequest, cache_control string) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling search request body: %w", err)
	}
	return rtcontext.Do("POST", "/v1/search", bytes.NewReader(jsonBody), "Content-Type", "application/json", "Cache-Control", cache_control)
}

func init() {
	searchCmd.Flags().String("cache-control", "cache", "Control whether the results cache is used for searches. [possible values: cache, no-cache]")
	searchCmd.Flags().String(constants.ModelKeyFlag, "", "Model to use for search")
	searchCmd.Flags().Uint(limitKeyFlag, 10, "Limit number of search results")
	searchCmd.Flags().String("endpoint", "", "Specifies the remote Spice instance HTTP endpoint (e.g., http://localhost:8090)")
	searchCmd.Flags().StringSlice("headers", []string{}, "Custom HTTP headers to pass to remote endpoint in the format 'Key:Value'. Can be specified multiple times.")

	RootCmd.AddCommand(searchCmd)
}
