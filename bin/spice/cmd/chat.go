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
	"context"
	"fmt"
	"log/slog"
	"os"
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

		httpEndpoint, err := cmd.Flags().GetString(httpEndpointKeyFlag)
		if err != nil {
			slog.Error("could not get http-endpoint flag", "error", err)
			os.Exit(1)
		}
		if httpEndpoint != "" {
			rtcontext.SetHttpEndpoint(httpEndpoint)
		}

		headers := []option.RequestOption{option.WithAPIKey(apiKey), option.WithBaseURL(fmt.Sprintf("%s/v1/", rtcontext.HttpEndpoint()))}
		for key, value := range rtcontext.GetHeaders() {
			headers = append(
				headers,
				option.WithHeader(key, value),
			)
		}
		client := openai.NewClient(
			headers...,
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
			receivedFirstChunk := make(chan bool)
			go func() {
				util.ShowSpinner(receivedFirstChunk)
			}()

			line.AppendHistory(message)
			messages = append(messages, openai.UserMessage(message))

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

			var usage *openai.CompletionUsage
			for stream.Next() {
				chunk := stream.Current()
				if timeAtFirstToken.IsZero() {
					timeAtFirstToken = time.Now()
					receivedFirstChunk <- true
				}

				if chunk.Usage.TotalTokens > 0 {
					usage = &chunk.Usage
				}

				if !acc.AddChunk(chunk) {
					slog.Error("Cannot accumulate stream of chat data")
					break
				}

				if content, ok := acc.JustFinishedContent(); ok {
					messages = append(messages, openai.SystemMessage(content))
				}

				if refusal, ok := acc.JustFinishedRefusal(); ok {
					slog.Error("Refused to answer", "refusal", refusal)
				}

				if len(chunk.Choices) > 0 {
					fmt.Printf("%s", chunk.Choices[0].Delta.Content)
				}
			}

			timeAtCompletion = time.Now()

			if usage != nil {
				cmd.Printf("\n\n%s\n\n", generateUsageMessage(
					usage,
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
func generateUsageMessage(u *openai.CompletionUsage, timeToFirst time.Duration, streamDuration time.Duration) string {
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

func init() {
	chatCmd.Flags().Bool(cloudKeyFlag, false, "Use cloud instance for chat (default: false)")
	chatCmd.Flags().String(modelKeyFlag, "", "Model to chat with")
	chatCmd.Flags().String(httpEndpointKeyFlag, "", "HTTP endpoint for chat (default: http://localhost:8090)")
	chatCmd.Flags().String(userAgentKeyFlag, "", "User agent to use in all requests")
	chatCmd.Flags().String("api-key", "", "The API key to use for authentication")

	RootCmd.AddCommand(chatCmd)
}
