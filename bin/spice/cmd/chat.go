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
	"encoding/json"
	"fmt"
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

const (
	cloudKeyFlag        = "cloud"
	modelKeyFlag        = "model"
	httpEndpointKeyFlag = "http-endpoint"
	userAgentKeyFlag    = "user-agent"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequestBody struct {
	Messages      []Message      `json:"messages"`
	Model         string         `json:"model"`
	Stream        bool           `json:"stream"`
	StreamOptions *StreamOptions `json:"stream_options"`
}

type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type Delta struct {
	Content      string      `json:"content"`
	FunctionCall interface{} `json:"function_call"`
	ToolCalls    interface{} `json:"tool_calls"`
	Role         interface{} `json:"role"`
}

type Choice struct {
	Index        int         `json:"index"`
	Delta        Delta       `json:"delta"`
	FinishReason interface{} `json:"finish_reason"`
	Logprobs     interface{} `json:"logprobs"`
}

type ChatCompletion struct {
	ID                string   `json:"id"`
	Choices           []Choice `json:"choices"`
	Created           int64    `json:"created"`
	Model             string   `json:"model"`
	SystemFingerprint string   `json:"system_fingerprint"`
	Object            string   `json:"object"`
	Usage             *Usage   `json:"usage"`
}

type Usage struct {
	CompletionTokens int `json:"completion_tokens"`
	PromptTokens     int `json:"prompt_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type OpenAIError struct {
	Message string `json:"message"`
}

type OpenAIErrorResponse struct {
	Error OpenAIError `json:"error"`
}

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
		rtcontext := context.NewContext().WithCloud(cloud)
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

		if !cloud {
			rtcontext.RequireModelsFlavor(cmd)
		}

		model, err := cmd.Flags().GetString(modelKeyFlag)
		if err != nil {
			slog.Error("could not get model flag", "error", err)
			os.Exit(1)
		}
		if model == "" {
			models, err := api.GetDataSingle[api.ModelResponse](rtcontext, "/v1/models?status=true")
			if err != nil {
				slog.Error("could not list models", "error", err)
				os.Exit(1)
			}

			if len(models.Data) == 0 {
				slog.Error("No models found")
				os.Exit(1)
			}

			availableModels := []string{}
			for _, model := range models.Data {
				if model.Status == "Ready" {
					availableModels = append(availableModels, model.Id)
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

		var messages []Message = []Message{}

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
			messages = append(messages, Message{Role: "user", Content: message})

			done := make(chan bool)
			go func() {
				util.ShowSpinner(done)
			}()

			body := &ChatRequestBody{
				Messages:      messages,
				Model:         model,
				Stream:        true,
				StreamOptions: &StreamOptions{IncludeUsage: true},
			}
			var timeAtCompletion time.Time
			var timeAtFirstToken time.Time
			startTime := time.Now()
			response, err := sendChatRequest(rtcontext, body)
			if err != nil {
				slog.Error("failed to send chat request to spiced", "error", err)
				continue
			}

			scanner := bufio.NewScanner(response.Body)
			var responseMessage = ""

			/// Usage for the entire stream, and related timing.
			var usage Usage
			doneLoading := false

			for scanner.Scan() {
				chunk := scanner.Text()
				if timeAtFirstToken.IsZero() {
					timeAtFirstToken = time.Now()
				}

				errorEvent, err := maybeErrorEvent(chunk, scanner)

				if err != nil {
					slog.Error("failed to decode error event", "error", err)
					continue
				}

				if errorEvent != nil {
					slog.Error("chat request failed", "error", errorEvent.Message)
					break
				}

				if !strings.HasPrefix(chunk, "data: ") {
					continue
				}
				chunk = strings.TrimPrefix(chunk, "data: ")

				var chatResponse ChatCompletion = ChatCompletion{}
				err = json.Unmarshal([]byte(chunk), &chatResponse)
				if err != nil {
					slog.Error("failed to unmarshal chat response", "error", err)
					continue
				}

				if !doneLoading {
					done <- true
					doneLoading = true
				}

				if chatResponse.Usage != nil {
					usage = *chatResponse.Usage
					timeAtCompletion = time.Now()
				}

				if len(chatResponse.Choices) == 0 {
					continue
				}

				token := chatResponse.Choices[0].Delta.Content
				cmd.Printf("%s", token)
				responseMessage = responseMessage + token
			}

			if err := scanner.Err(); err != nil {
				slog.Error("error occurred while processing the input stream", "error", err)
			}

			if !doneLoading {
				done <- true
				doneLoading = true
			}

			if responseMessage != "" {
				messages = append(messages, Message{Role: "assistant", Content: responseMessage})
			}
			if usage != (Usage{}) {
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

	RootCmd.AddCommand(chatCmd)
}
