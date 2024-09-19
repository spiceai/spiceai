package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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
	Datasets          []string `json:"datasets"`
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
spice search --model <model>

# Start a search session with spiced instance in spice.ai cloud
spice search --model <model> --cloud
`,
	Run: func(cmd *cobra.Command, args []string) {
		cloud, _ := cmd.Flags().GetBool(cloudKeyFlag)
		rtcontext := context.NewContext().WithCloud(cloud)

		rtcontext.RequireModelsFlavor(cmd)

		datasets, err := api.GetData[api.Dataset](rtcontext, "/v1/datasets?status=true")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
		var searchDatasets []string
		for _, dataset := range datasets {
			if strings.HasSuffix(dataset.Name, ".docs") {
				continue
			}
			searchDatasets = append(searchDatasets, dataset.Name)
		}

		httpEndpoint, err := cmd.Flags().GetString("http-endpoint")
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		matches := map[string][]SearchMatch{}

		limit, err := cmd.Flags().GetUint(limitKeyFlag)
		if err != nil {
			cmd.Println(err)
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
				log.Print("Error reading line: ", err)
				continue
			}

			line.AppendHistory(message)

			done := make(chan bool)
			go func() {
				util.ShowSpinner(done)
			}()

			body := &SearchRequest{
				Text:     message,
				Datasets: searchDatasets,
				Limit:    limit,
			}

			response, err := sendSearchRequest(rtcontext, body)
			if err != nil {
				cmd.Printf("Error: %v\n", err)
				continue
			}

			scanner := bufio.NewScanner(response.Body)

			doneLoading := false

			for scanner.Scan() {
				chunk := scanner.Text()

				var searchResponse SearchResponse = SearchResponse{}
				err = json.Unmarshal([]byte(chunk), &searchResponse)
				if err != nil {
					cmd.Printf("Error: %v\n\n", err)
					continue
				}

				if !doneLoading {
					done <- true
					doneLoading = true
				}

				for i, match := range searchResponse.Matches {
					cmd.Printf("%d. [%s] %s\n\n", i+1, match.Dataset, match.Value)
				}
				matches[message] = append(matches[message], searchResponse.Matches...)
				cmd.Printf("Time: %s. %d results.", time.Duration(searchResponse.DurationMs)*time.Millisecond, len(searchResponse.Matches))
			}

			if !doneLoading {
				done <- true
				doneLoading = true
			}

			cmd.Print("\n\n")
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

	request.Header = rtcontext.GetHeaders()

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
