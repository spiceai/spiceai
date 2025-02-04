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

	"github.com/manifoldco/promptui"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type NsqlRequest struct {
	Query string `json:"query"`
	Model string `json:"model"`
}

type NsqlResponse struct {
	Matches    []SearchMatch `json:"matches"`
	DurationMs uint64        `json:"duration_ms"`
}

var nsqlCmd = &cobra.Command{
	Use:   "nsql",
	Short: "Text-to-SQL REPL",
	Example: `
$ spice nsql
Welcome to the Spice.ai NSQL REPL!

Using model:
 openai

Enter a query in natural language.
nsql> How much money have I made in each country?
+-------------+--------------------+
| country     | total_sales        |
+-------------+--------------------+
| USA         | 3627982.83         |
| Spain       | 1215686.9200000009 |
| France      | 1110916.5199999993 |
| Australia   | 630623.1000000001  |
| UK          | 478880.4600000001  |
+-------------+--------------------+
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println("Welcome to the Spice.ai NSQL REPL!")

		cloud, _ := cmd.Flags().GetBool(cloudKeyFlag)
		rtcontext := context.NewContext().WithCloud(cloud)

		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		userAgent, _ := cmd.Flags().GetString("user-agent")
		if userAgent != "" {
			rtcontext.SetUserAgent(userAgent)
		} else {
			rtcontext.SetUserAgentClient("nsql")
		}

		rtcontext.RequireModelsFlavor(cmd)

		model, err := cmd.Flags().GetString(modelKeyFlag)
		if err != nil {
			slog.Error("getting model flag", "error", err)
			os.Exit(1)
		}
		if model == "" {
			models, err := api.GetDataSingle[api.ModelResponse](rtcontext, "/v1/models?status=true")
			if err != nil {
				slog.Error("listing spiced models", "error", err)
				os.Exit(1)
			}
			if len(models.Data) == 0 {
				slog.Error("no models found")
				os.Exit(1)
			}

			modelsSelection := []string{}
			selectedModel := models.Data[0].Id
			if len(models.Data) > 1 {
				for _, model := range models.Data {
					modelsSelection = append(modelsSelection, model.Id)
				}

				prompt := promptui.Select{
					Label:        "Select model",
					Items:        modelsSelection,
					HideSelected: true,
				}

				_, selectedModel, err = prompt.Run()
				if err != nil {
					fmt.Printf("Prompt failed %v\n", err)
					return
				}
			}

			fmt.Println("Using model:\n", selectedModel)
			model = selectedModel
		}

		httpEndpoint, err := cmd.Flags().GetString(httpEndpointKeyFlag)
		if err != nil {
			slog.Error("getting http-endpoint flag", "error", err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		cmd.Println("")
		cmd.Println("Enter a query in natural language.")

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer line.Close()
		for {
			message, err := line.Prompt("nsql> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err != nil {
				slog.Error("reading line", "error", err)
				continue
			}

			if strings.Trim(message, " ") == "" {
				cmd.Println("Enter a No-SQL (natural language) query.")
				continue
			}

			line.AppendHistory(message)
			done := make(chan bool)
			go func() {
				util.ShowSpinner(done)
			}()

			responses := make(chan *http.Response)
			start := time.Now()
			go func(out chan *http.Response) {
				defer close(out)
				response, err := sendNsqlRequest(rtcontext, &NsqlRequest{
					Query: message,
					Model: model,
				})
				if err != nil {
					slog.Error("sending nsql request", "error", err)
					out <- nil
				} else {
					out <- response
				}
			}(responses)

			response := <-responses
			time := time.Since(start).Seconds()
			done <- true
			if response == nil {
				// Error already printed in goroutine
				continue
			}

			raw, err := io.ReadAll(response.Body)
			if err != nil {
				slog.Error("reading response body", "error", err)
				continue
			}

			output := string(raw)

			// Three newlines are from the header, and two spacing +-------------+` to form table.
			numberRows := strings.Count(output, "\n") - 3

			if output == "++\n++" {
				cmd.Printf("No results.\n")
			} else if response.StatusCode != 200 {
				cmd.Printf("\033[31mQueryError\033[0m %s\n", output)
			} else {
				cmd.Printf("%s\n\nTime: %f seconds. %d rows.\n", output, time, numberRows)
			}
		}
	},
}

func sendNsqlRequest(rtcontext *context.RuntimeContext, body *NsqlRequest) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling nsql request body: %w", err)
	}

	url := fmt.Sprintf("%s/v1/nsql", rtcontext.HttpEndpoint())
	request, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating nsql request: %w", err)
	}

	headers := rtcontext.GetHeaders()
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/plain")

	response, err := rtcontext.Client().Do(request)
	if err != nil {
		return nil, fmt.Errorf("error sending nsql request: %w", err)
	}

	return response, nil
}

func init() {
	nsqlCmd.Flags().Bool(cloudKeyFlag, false, "Use cloud instance for nsql (default: false)")
	nsqlCmd.Flags().String(modelKeyFlag, "", "Model to use for nsql")
	nsqlCmd.Flags().String(httpEndpointKeyFlag, "", "HTTP endpoint for nsql (default: http://localhost:8090)")
	nsqlCmd.Flags().String("user-agent", "", "User agent to use in all requests")

	RootCmd.AddCommand(nsqlCmd)
}
