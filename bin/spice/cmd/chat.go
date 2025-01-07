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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	spiceContext "github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	cloudKeyFlag        = "cloud"
	modelKeyFlag        = "model"
	httpEndpointKeyFlag = "http-endpoint"
	userAgentKeyFlag    = "user-agent"
)

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Chat with the Spice.ai LLM agent",
	Example: `
# Start a chat session with local spiced instance
spice chat --model <model>

# Start a chat session with spiced instance in spice.ai cloud
spice chat --model <model> --cloud
`,
	Run: func(cmd *cobra.Command, args []string) {
		cloud, _ := cmd.Flags().GetBool(cloudKeyFlag)
		rtcontext := spiceContext.NewContext().WithCloud(cloud)
		err := rtcontext.Init()
		if err != nil {
			slog.Error("could not initialize runtime context", "error", err)
			os.Exit(1)
		}

		apiKey, _ := cmd.Flags().GetString("api-key")
		if apiKey != "" {
			rtcontext.SetApiKey(apiKey)
		}

		userAgent, _ := cmd.Flags().GetString(userAgentKeyFlag)
		if userAgent != "" {
			rtcontext.SetUserAgent(userAgent)
		} else {
			rtcontext.SetUserAgentClient("chat")
		}

		rtcontext.RequireModelsFlavor(cmd)

		model, err := cmd.Flags().GetString(modelKeyFlag)
		if err != nil {
			slog.Error("could not get model flag", "error", err)
			os.Exit(1)
		}
		if model == "" {
			models, err := api.GetData[api.Model](rtcontext, "/v1/models?status=true")
			if err != nil {
				slog.Error("could not list models", "error", err)
				os.Exit(1)
			}

			if len(models) == 0 {
				slog.Error("No models found")
				os.Exit(1)
			}

			availableModels := []string{}
			for _, model := range models {
				if model.Status == "Ready" {
					availableModels = append(availableModels, model.Name)
				}
			}

			if len(availableModels) == 0 {
				slog.Error("No models are ready")
				os.Exit(1)
			}

			selectedModel := availableModels[0]
			if len(availableModels) > 1 {

				prompt := promptui.Select{
					Label:        "Select model",
					Items:        availableModels,
					HideSelected: true,
				}

				_, selectedModel, err = prompt.Run()
				if err != nil {
					slog.Error("prompt failed", "error", err)
					return
				}
			}

			cmd.Printf("Using model: %s\n", selectedModel)
			model = selectedModel
		}

		httpEndpoint, err := cmd.Flags().GetString("http-endpoint")
		if err != nil {
			slog.Error("could not get http-endpoint flag", "error", err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		client := openai.NewClient(
			option.WithBaseURL(httpEndpoint),
			option.WithAPIKey(apiKey),
			option.WithHeader("User-Agent", userAgent),
		)

		var messages []openai.ChatCompletionMessageParamUnion

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer line.Close()
		for {
			message, err := line.Prompt("chat> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err != nil {
				slog.Error("reading input line", "error", err)
				continue
			}

			line.AppendHistory(message)
			messages = append(messages, openai.UserMessage(message))

			done := make(chan bool)
			go func() {
				util.ShowSpinner(done)
			}()

			var timeAtCompletion time.Time
			var timeAtFirstToken time.Time
			startTime := time.Now()

			stream := client.Chat.Completions.NewStreaming(
				context.Background(),
				openai.ChatCompletionNewParams{
					Messages: openai.F(messages),
					Model:    openai.F(model),
					StreamOptions: openai.F(openai.ChatCompletionStreamOptionsParam{
						IncludeUsage: openai.F(true),
					}),
				},
			)
			acc := openai.ChatCompletionAccumulator{}
			var usage openai.CompletionUsage
			doneLoading := false

			for stream.Next() {
				chunk := stream.Current()
				if timeAtFirstToken.IsZero() {
					timeAtFirstToken = time.Now()
					if !doneLoading {
						done <- true
						doneLoading = true
					}
				}
				acc.AddChunk(chunk)

				// When this fires, the current chunk value will not contain content data
				if content, ok := acc.JustFinishedContent(); ok {
					messages = append(messages, openai.SystemMessage(content))
				}

				if tool, ok := acc.JustFinishedToolCall(); ok {
					println("Tool call stream finished:", tool.Index, tool.Name, tool.Arguments)
					// TODO: add tool call completion into `messages`.
					//
					println()
				}

				if refusal, ok := acc.JustFinishedRefusal(); ok {
					fmt.Printf("Refusal: %v\n\n", refusal)

				}

				println(chunk.Choices[0].Delta.Content)
			}

			usage = acc.Usage
			timeAtCompletion = time.Now()

			if usage.PromptTokens > 0 && usage.CompletionTokens > 0 {
				cmd.Printf("\n\n%s\n\n", generateUsageMessage(
					&usage,
					timeAtFirstToken.Sub(startTime).Abs(),
					timeAtCompletion.Sub(timeAtFirstToken).Abs(),
				))
			} else {
				cmd.Print("\n\n")
			}
		}
	},
}

// `generateUsageMessage` generates a boxed summary of the usage statistics.
//
// ```shell
// Time: 3.36s (first token 0.45s). Tokens: 1652. Prompt: 1475. Completion: 177 (292.25/s).
// ```
// If no usage data provided:
// ```shell
// Time: 3.36s (first token 0.45s).
// ```
func generateUsageMessage(u *Usage, timeToFirst time.Duration, streamDuration time.Duration) string {
	totalTime := (streamDuration + timeToFirst)
	times := fmt.Sprintf("Time: %.2fs (first token %.2fs).", totalTime.Seconds(), timeToFirst.Seconds())
	if u == nil {
		return times
	}

	tps := float64(u.CompletionTokens) / (streamDuration.Seconds())
	return fmt.Sprintf(
		"%s Tokens: %d. Prompt: %d. Completion: %d (%.2f/s).", times, u.TotalTokens, u.PromptTokens, u.CompletionTokens, tps,
	)
}

func sendChatRequest(rtcontext *context.RuntimeContext, body *ChatRequestBody) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request body: %w", err)
	}

	url := fmt.Sprintf("%s/v1/chat/completions", rtcontext.HttpEndpoint())
	request, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
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

func maybeErrorEvent(chunk string, scanner *bufio.Scanner) (*OpenAIError, error) {
	if strings.HasPrefix(chunk, "event: error") {
		scanner.Scan() // read line with error message
		errorMessage := scanner.Text()
		errorMessage = strings.TrimPrefix(errorMessage, "data: ")

		var errorResponse OpenAIErrorResponse = OpenAIErrorResponse{}
		err := json.Unmarshal([]byte(errorMessage), &errorResponse)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal: %w", err)
		}

		return &errorResponse.Error, nil
	}

	return nil, nil
}

func init() {
	chatCmd.Flags().Bool(cloudKeyFlag, false, "Use cloud instance for chat (default: false)")
	chatCmd.Flags().String(modelKeyFlag, "", "Model to chat with")
	chatCmd.Flags().String(httpEndpointKeyFlag, "", "HTTP endpoint for chat (default: http://localhost:8090)")
	chatCmd.Flags().String(userAgentKeyFlag, "", "User agent to use in all requests")
	chatCmd.Flags().String("api-key", "", "The API key to use for authentication")

	RootCmd.AddCommand(chatCmd)
}
