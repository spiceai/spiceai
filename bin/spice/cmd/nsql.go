package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
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
# Start a text-to-sql REPL session with local spiced instance
spice nsql

# Start a text-to-sql REPL session with spiced instance in spice.ai cloud
spice nsql --cloud
`,
	Run: func(cmd *cobra.Command, args []string) {
		cloud, _ := cmd.Flags().GetBool(cloudKeyFlag)
		rtcontext := context.NewContext().WithCloud(cloud)

		rtcontext.RequireModelsFlavor(cmd)

		model, err := cmd.Flags().GetString(modelKeyFlag)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
		if model == "" {
			models, err := api.GetData[api.Model](rtcontext, "/v1/models?status=true")
			if err != nil {
				cmd.PrintErrln(err.Error())
				os.Exit(1)
			}
			if len(models) == 0 {
				cmd.Println("No models found")
				os.Exit(1)
			}

			modelsSelection := []string{}
			selectedModel := models[0].Name
			if len(models) > 1 {
				for _, model := range models {
					modelsSelection = append(modelsSelection, model.Name)
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

			fmt.Println("Using model:", selectedModel)
			fmt.Println()
			model = selectedModel
		}

		httpEndpoint, err := cmd.Flags().GetString("http-endpoint")
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer line.Close()
		for {
			message, err := line.Prompt("nsql> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err != nil {
				log.Print("Error reading line: ", err)
				continue
			}

			if strings.Trim(message, " ") == "" {
				cmd.Println("Please enter a No-SQL query.")
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
				response, err := sendNsqlRequest(rtcontext, &NsqlRequest{
					Query: message,
					Model: model,
				})
				log.Printf("Response: %v\n", response)
				if err != nil {
					cmd.Printf("Error: %v\n", err)
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
				cmd.Printf("Error: %v\n\n", err)
				continue
			}
			var searchResponse SearchResponse = SearchResponse{}
			err = json.Unmarshal([]byte(raw), &searchResponse)
			if err != nil {
				cmd.Printf("Error: %v\n\n", err)
				continue
			}

			cmd.Printf("Time: %s. %d results.\n\n", time.Duration(searchResponse.DurationMs)*time.Millisecond, len(searchResponse.Matches))
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

	request.Header = rtcontext.GetHeaders()

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
	nsqlCmd.Flags().Uint(limitKeyFlag, 10, "Limit number of results")

	RootCmd.AddCommand(nsqlCmd)
}
