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

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	limitKeyFlag = "limit"
)

type SearchRequest struct {
	Text              string   `json:"text"`
	Datasets          []string `json:"datasets,omitempty"`
	Limit             uint     `json:"limit"`
	AdditionalColumns []string `json:"additional_columns,omitempty"`
	Where             string   `json:"where,omitempty"`
}

type SearchMatch struct {
	Value      string                 `json:"value"`
	Score      float64                `json:"score"`
	Dataset    string                 `json:"dataset"`
	PrimaryKey map[string]interface{} `json:"primary_key"`
	Metadata   map[string]interface{} `json:"metadata"`
}

type SearchResponse struct {
	Matches    []SearchMatch `json:"matches"`
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
		cloud, _ := cmd.Flags().GetBool(cloudKeyFlag)
		rtcontext := context.NewContext().WithCloud(cloud)

		if !cloud {
			rtcontext.RequireModelsFlavor(cmd)
		}

		datasets, err := api.GetDatasetsWithStatus(rtcontext)
		if err != nil {
			slog.Error("could not list datasets", "error", err)
		}

		for _, dataset := range datasets {
			if dataset.Status != api.Ready.String() && dataset.Status != api.Refreshing.String() {
				// warn only if vector_search is supported by the dataset
				prop_val, _ := dataset.GetPropertyValue("vector_search")
				if prop_val == "supported" {
					slog.Warn(fmt.Sprintf("Dataset %s is not ready (%s) and will be excluded from the search.", dataset.Name, dataset.Status))
				}
			}
		}

		httpEndpoint, err := cmd.Flags().GetString("http-endpoint")
		if err != nil {
			slog.Error("could not get http-endpoint flag", "error", err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		matches := map[string][]SearchMatch{}

		limit, err := cmd.Flags().GetUint(limitKeyFlag)
		if err != nil {
			slog.Error("could not get limit flag", "error", err)
			os.Exit(1)
		}

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer line.Close()
		for {
			message, err := line.Prompt("search> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err != nil {
				slog.Error("reading input line", "error", err)
				continue
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
				})
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

			var searchResponse SearchResponse = SearchResponse{}
			err = json.Unmarshal([]byte(raw), &searchResponse)
			if err != nil {
				slog.Error("parsing response from spiced", "error", err)
				continue
			}

			for i, match := range searchResponse.Matches {
				cmd.Printf("Rank %d, Score: %0.1f, Datasets [%s]", i+1, match.Score*100, match.Dataset)
				if len(match.PrimaryKey) > 0 {
					for key, value := range match.PrimaryKey {
						cmd.Printf(" %s=%v", key, value)
					}
				}
				cmd.Printf("\n%s\n\n", match.Value)
			}

			matches[message] = append(matches[message], searchResponse.Matches...)
			cmd.Printf("Time: %s. %d results.\n\n", time.Duration(searchResponse.DurationMs)*time.Millisecond, len(searchResponse.Matches))
		}
	},
}

func sendSearchRequest(rtcontext *context.RuntimeContext, body *SearchRequest) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling search request body: %w", err)
	}

	url := fmt.Sprintf("%s/v1/search", rtcontext.HttpEndpoint())
	request, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating search request: %w", err)
	}

	headers := rtcontext.GetHeaders()
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := rtcontext.Client().Do(request)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}

	return response, nil
}

func init() {
	searchCmd.Flags().Bool(cloudKeyFlag, false, "Use cloud instance for search (default: false)")
	searchCmd.Flags().String(modelKeyFlag, "", "Model to use for search")
	searchCmd.Flags().String(httpEndpointKeyFlag, "", "HTTP endpoint for search (default: http://localhost:8090)")
	searchCmd.Flags().Uint(limitKeyFlag, 10, "Limit number of search results")

	RootCmd.AddCommand(searchCmd)
}
