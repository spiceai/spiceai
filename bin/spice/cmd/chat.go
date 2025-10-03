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
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	spice_http "github.com/spiceai/spiceai/bin/spice/pkg/http"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
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
	ChatRequestOptions
}

// ChatRequestOptions contains all optional fields for chat requests
type ChatRequestOptions struct {
	Temperature *float32 `json:"temperature,omitempty"`
}

func NewChatRequestBody(messages []Message, model string, stream bool, streamOptions *StreamOptions) *ChatRequestBody {
	return &ChatRequestBody{
		Messages:      messages,
		Model:         model,
		Stream:        stream,
		StreamOptions: streamOptions,
	}
}

func ApplyChatOptions(body *ChatRequestBody, cmd *cobra.Command) (*ChatRequestBody, error) {
	if cmd.Flags().Changed("temperature") {
		temperature, err := cmd.Flags().GetFloat32("temperature")
		if err != nil {
			slog.Error("could not get temperature flag", "error", err)
			os.Exit(1)
		}
		if temperature < 0 {
			slog.Error("temperature must be greater than or equal to 0")
			os.Exit(1)
		}
		body.Temperature = &temperature
	}

	return body, nil
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

type ResponsesRequestBody struct {
	Model  string `json:"model"`
	Input  string `json:"input"`
	Stream *bool  `json:"stream,omitempty"`
}

func NewResponsesRequestBody(model string, input string, stream bool) *ResponsesRequestBody {
	return &ResponsesRequestBody{
		Model:  model,
		Input:  input,
		Stream: &stream,
	}
}

type ResponseOutput struct {
	Type    string                 `json:"type"`
	ID      string                 `json:"id"`
	Status  string                 `json:"status"`
	Role    string                 `json:"role"`
	Content []ResponseContentBlock `json:"content"`
}

type ResponseContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type ResponseUsage struct {
	InputTokens         int `json:"input_tokens"`
	InputTokensDetails  any `json:"input_tokens_details"`
	OutputTokens        int `json:"output_tokens"`
	OutputTokensDetails any `json:"output_tokens_details"`
	TotalTokens         int `json:"total_tokens"`
}

type ResponsesAPIResponse struct {
	ID        string           `json:"id"`
	Object    string           `json:"object"`
	CreatedAt int64            `json:"created_at"`
	Status    string           `json:"status"`
	Model     string           `json:"model"`
	Output    []ResponseOutput `json:"output"`
	Usage     *ResponseUsage   `json:"usage"`
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

type ResponsesAPIErrorResponse struct {
	Type  string      `json:"type"`
	Error OpenAIError `json:"error"`
}

type ResponseStreamEvent struct {
	Type           string                `json:"type"`
	ItemID         string                `json:"item_id,omitempty"`
	OutputIndex    int                   `json:"output_index,omitempty"`
	ContentIndex   int                   `json:"content_index,omitempty"`
	Delta          string                `json:"delta,omitempty"`
	SequenceNumber int                   `json:"sequence_number,omitempty"`
	Usage          *ResponseUsage        `json:"usage,omitempty"`
	Response       *ResponsesAPIResponse `json:"response,omitempty"`
}

func handleResponsesAPI(rtcontext *context.RuntimeContext, cmd *cobra.Command, model string, messages []Message, useSpinner bool) ([]Message, error) {
	input := messagesToInput(messages)

	// Show spinner if requested
	var done chan bool
	var doneLoading bool

	if useSpinner {
		done = make(chan bool)
		doneLoading = false
		go func() {
			util.ShowSpinner(done)
		}()
	}

	body := NewResponsesRequestBody(model, input, true)

	var timeAtCompletion time.Time
	var timeAtFirstToken time.Time
	startTime := time.Now()
	response, err := sendResponsesRequest(rtcontext, body)
	if err != nil {
		if useSpinner {
			done <- true
		}
		slog.Error("failed to send responses request to spiced", "error", err)
		return messages, fmt.Errorf("failed to send responses request: %w", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			slog.Error("failed to close response body", "error", err)
		}
	}()

	scanner := bufio.NewScanner(response.Body)
	var responseMessage = ""

	var usage ResponseUsage

	if useSpinner {
		doneLoading = false
	}

	for scanner.Scan() {
		chunk := scanner.Text()
		if timeAtFirstToken.IsZero() {
			timeAtFirstToken = time.Now()
		}

		if !strings.HasPrefix(chunk, "data: ") {
			continue
		}
		chunk = strings.TrimPrefix(chunk, "data: ")

		if isEndOfStream(chunk) {
			break
		}

		responsesAPIError, err := maybeResponsesAPIErrorEvent(chunk)
		if err != nil {
			slog.Error("failed to decode responses API error event", "error", err)
			continue
		}

		if responsesAPIError != nil {
			slog.Error("responses request failed", "message", responsesAPIError.Error.Message)
			break
		}

		var streamEvent = ResponseStreamEvent{}
		err = json.Unmarshal([]byte(chunk), &streamEvent)
		if err != nil {
			slog.Error("failed to unmarshal responses stream event", "error", err)
			continue
		}

		if streamEvent.Usage != nil {
			usage = *streamEvent.Usage
			timeAtCompletion = time.Now()
		}

		if streamEvent.Type == "response.output_text.delta" {
			token := streamEvent.Delta

			// Stop spinner on first non-empty text delta
			if useSpinner && !doneLoading && token != "" {
				done <- true
				doneLoading = true
			}

			cmd.Printf("%s", token)
			responseMessage = responseMessage + token
		}

		if streamEvent.Type == "response.completed" && streamEvent.Response != nil {
			if streamEvent.Response.Usage != nil {
				usage = *streamEvent.Response.Usage
				timeAtCompletion = time.Now()
			}
		}

		if streamEvent.Type == "response.done" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("error occurred while processing the response stream", "error", err)
	}

	if useSpinner && !doneLoading {
		done <- true
	}

	if responseMessage != "" {
		messages = append(messages, Message{Role: "assistant", Content: responseMessage})
	}

	// Show usage information
	if usage != (ResponseUsage{}) {
		// If timeAtCompletion wasn't set, use current time
		if timeAtCompletion.IsZero() {
			timeAtCompletion = time.Now()
		}
		cmd.Printf("\n\n%s\n\n", generateResponsesUsageMessage(
			&usage,
			timeAtFirstToken.Sub(startTime).Abs(),
			timeAtCompletion.Sub(timeAtFirstToken).Abs(),
		))
	} else {
		cmd.Print("\n\n")
	}

	return messages, nil
}

// handleChatCompletions handles streaming responses using the Chat Completions API
func handleChatCompletions(rtcontext *context.RuntimeContext, cmd *cobra.Command, model string, messages []Message, useSpinner bool) ([]Message, error) {
	// Only create these variables if using spinner
	var done chan bool
	var doneLoading bool

	if useSpinner {
		done = make(chan bool)
		doneLoading = false
		go func() {
			util.ShowSpinner(done)
		}()
	}

	body := NewChatRequestBody(messages, model, true, &StreamOptions{
		IncludeUsage: true,
	})
	body, _ = ApplyChatOptions(body, cmd)

	var timeAtCompletion time.Time
	var timeAtFirstToken time.Time
	startTime := time.Now()
	response, err := sendChatRequest(rtcontext, body)
	if err != nil {
		slog.Error("failed to send chat request to spiced", "error", err)
		return messages, fmt.Errorf("failed to send chat request: %w", err)
	}

	scanner := bufio.NewScanner(response.Body)
	var responseMessage = ""

	/// Usage for the entire stream, and related timing.
	var usage Usage

	if useSpinner {
		doneLoading = false
	}

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

		var chatResponse = ChatCompletion{}
		err = json.Unmarshal([]byte(chunk), &chatResponse)
		if err != nil {
			slog.Error("failed to unmarshal chat response", "error", err)
			continue
		}

		if useSpinner && !doneLoading {
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
		slog.Error("error occurred while processing the response stream", "error", err)
	}

	if useSpinner && !doneLoading {
		done <- true
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

	return messages, nil
}

var chatCmd = &cobra.Command{
	Use:   "chat [flags] [message]",
	Short: "Chat with the Spice.ai LLM agent",
	Long: `Chat with the Spice.ai LLM agent.
	With no message argument: starts an interactive chat session.
	With one message argument: sends the message and exits.`,
	Example: `
# Start a chat session with local spiced instance
spice chat --model <model>

# Start a chat session with spiced instance in spice.ai cloud
spice chat --model <model> --cloud

# Send a single prompt and receive a response
spice chat --model <model> "What is Spice.ai?"
`,
	Args: cobra.MaximumNArgs(1),
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
			runRemoteChatREPL(cmd, rtcontext, "https://data.spiceai.io", args)
			return
		}

		if endpoint != "" {
			// Remote HTTP mode
			runRemoteChatREPL(cmd, rtcontext, endpoint, args)
			return
		}

		temperature, err := cmd.Flags().GetFloat32("temperature")
		if err != nil {
			slog.Error("could not get temperature flag", "error", err)
			os.Exit(1)
		}
		if temperature < 0 {
			slog.Error("temperature must be greater than or equal to 0")
			os.Exit(1)
		}

		if !rtcontext.IsCloud() {
			rtcontext.RequireModelsFlavor(cmd)
		}

		model, err := cmd.Flags().GetString("model")
		if err != nil {
			slog.Error("could not get model flag", "error", err)
			os.Exit(1)
		}

		models, err := api.GetDataSingle[api.ModelResponse](rtcontext, "/v1/models?status=true&metadata_fields=supports_responses_api")
		if err != nil {
			slog.Error("could not list models", "error", err)
			os.Exit(1)
		}

		if len(models.Data) == 0 {
			slog.Error("No models found")
			os.Exit(1)
		}

		// Check if responses API should be used
		useResponsesAPI, err := cmd.Flags().GetBool("responses")
		if err != nil {
			slog.Error("could not get responses flag", "error", err)
			os.Exit(1)
		}

		availableModels := []string{}
		for _, model := range models.Data {
			if model.Status == "Ready" {
				if !useResponsesAPI || model.Metadata.SupportsResponsesAPI {
					availableModels = append(availableModels, model.Id)
				}
			}
		}

		if model == "" {
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
		} else {
			modelIsConfigured := false
			for _, m := range models.Data {
				if m.Id == model {
					modelIsConfigured = true
					break
				}
			}

			if !modelIsConfigured {
				ids := make([]string, len(models.Data))
				for i, m := range models.Data {
					ids[i] = m.Id
				}

				slog.Error(fmt.Sprintf("model %s does not exist — configured models: %s",
					model, strings.Join(ids, ", ")))

				os.Exit(1)
			}

			modelIsReady := false
			for _, m := range availableModels {
				if m == model {
					modelIsReady = true
					break
				}
			}
			if !modelIsReady {
				slog.Error(fmt.Sprintf("model %s is not ready — try again when ready",
					model))
				os.Exit(1)
			}
		}

		// Handler for Responses API - handled by standalone function

		// Handler for Chat Completions API - handled by standalone function

		// Main message handler that delegates to the appropriate API handler
		getChatResponse := func(messages []Message, useSpinner bool) ([]Message, error) {
			if useResponsesAPI {
				return handleResponsesAPI(rtcontext, cmd, model, messages, useSpinner)
			}
			return handleChatCompletions(rtcontext, cmd, model, messages, useSpinner)
		}

		if len(args) > 0 {
			userMessage := args[0]

			var messages = []Message{
				{Role: "user", Content: userMessage},
			}

			_, err = getChatResponse(messages, false)
			if err != nil {
				os.Exit(1)
			}

			return
		}

		var messages = []Message{}

		cmd.Println("Welcome to the Spice.ai chat REPL! Type your message to chat with the model.")
		cmd.Println()

		line := liner.NewLiner()
		line.SetCtrlCAborts(true)
		defer func() {
			if err := line.Close(); err != nil {
				slog.Error("closing line", "error", err)
			}
		}()
		for {
			message, err := line.Prompt("chat> ")
			if err == liner.ErrPromptAborted {
				break
			} else if err == io.EOF {
				// EOF reached (Ctrl+D or piped input exhausted)
				break
			} else if err != nil {
				slog.Error("reading input line", "error", err)
				break
			}

			line.AppendHistory(message)
			messages = append(messages, Message{Role: "user", Content: message})

			messages, err = getChatResponse(messages, true)
			if err != nil {
				continue
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
	return rtcontext.Do("POST", "/v1/chat/completions", bytes.NewReader(jsonBody), "Content-Type", "application/json")
}

func sendResponsesRequest(rtcontext *context.RuntimeContext, body *ResponsesRequestBody) (*http.Response, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request body: %w", err)
	}
	return rtcontext.Do("POST", "/v1/responses", bytes.NewReader(jsonBody), "Content-Type", "application/json")
}

// messagesToInput converts a message history into a single input string for the Responses API
func messagesToInput(messages []Message) string {
	var parts []string
	for _, msg := range messages {
		switch msg.Role {
		case "user":
			parts = append(parts, fmt.Sprintf("User: %s", msg.Content))
		case "assistant":
			parts = append(parts, fmt.Sprintf("Assistant: %s", msg.Content))
		case "system":
			parts = append(parts, fmt.Sprintf("System: %s", msg.Content))
		}
	}
	return strings.Join(parts, "\n\n")
}

// generateResponsesUsageMessage generates a usage message for Responses API statistics
func generateResponsesUsageMessage(u *ResponseUsage, timeToFirst time.Duration, streamDuration time.Duration) string {
	totalTime := (streamDuration + timeToFirst)
	times := fmt.Sprintf("Time: %.2fs (first token %.2fs).", totalTime.Seconds(), timeToFirst.Seconds())
	if u == nil {
		return times
	}

	tps := float64(u.OutputTokens) / streamDuration.Seconds()
	return fmt.Sprintf(
		"%s Tokens: %d. Input: %d. Output: %d (%.2f/s).", times, u.TotalTokens, u.InputTokens, u.OutputTokens, tps,
	)
}

func maybeErrorEvent(chunk string, scanner *bufio.Scanner) (*OpenAIError, error) {
	if strings.HasPrefix(chunk, "event: error") {
		scanner.Scan() // read line with error message
		errorMessage := scanner.Text()
		errorMessage = strings.TrimPrefix(errorMessage, "data: ")

		var errorResponse = OpenAIErrorResponse{}
		err := json.Unmarshal([]byte(errorMessage), &errorResponse)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal: %w", err)
		}

		return &errorResponse.Error, nil
	}

	return nil, nil
}

func maybeResponsesAPIErrorEvent(chunk string) (*ResponsesAPIErrorResponse, error) {
	var streamEvent ResponseStreamEvent
	err := json.Unmarshal([]byte(chunk), &streamEvent)
	if err != nil {
		return nil, err
	}

	if streamEvent.Type == "error" {
		var errorEvent ResponsesAPIErrorResponse
		err := json.Unmarshal([]byte(chunk), &errorEvent)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal responses API error: %w", err)
		}
		return &errorEvent, nil
	}

	return nil, nil
}

// End of Stream in the Responses API is indicated by an error event with text "stream ended" contained within it
func isEndOfStream(chunk string) bool {
	var streamEvent ResponseStreamEvent
	err := json.Unmarshal([]byte(chunk), &streamEvent)
	if err != nil {
		return false
	}

	if streamEvent.Type == "error" {
		var errorEvent ResponsesAPIErrorResponse
		err := json.Unmarshal([]byte(chunk), &errorEvent)
		if err != nil {
			return false
		}

		if strings.Contains(strings.ToLower(errorEvent.Error.Message), "stream ended") {
			return true
		}

	}

	return false
}

func runRemoteChatREPL(cmd *cobra.Command, rtcontext *context.RuntimeContext, httpEndpoint string, args []string) {
	// Get API key from context or environment variable
	apiKey := os.Getenv("SPICE_API_KEY")
	if apiKey == "" {
		if cmdApiKey, err := rtcontext.GetApiKey(); err == nil && cmdApiKey != "" {
			apiKey = cmdApiKey
		}
	}

	model, err := cmd.Flags().GetString("model")
	if err != nil {
		slog.Error("could not get model flag", "error", err)
		os.Exit(1)
	}

	if model == "" {
		slog.Error("--model flag is required for remote chat")
		os.Exit(1)
	}

	useResponsesAPI, err := cmd.Flags().GetBool("responses")
	if err != nil {
		slog.Error("could not get responses flag", "error", err)
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

	// Check server health and readiness
	checkDuration, healthOk := util.CheckRemoteServerHealth(httpEndpoint, httpClient, apiKey)
	if healthOk {
		cmd.Printf("Connected to %s (%dms).\n", httpEndpoint, checkDuration.Milliseconds())
	}
	cmd.Println()

	// Function to send chat request
	sendChatRequest := func(messages []Message, useSpinner bool) ([]Message, error) {
		var done chan bool
		var doneLoading bool
		if useSpinner {
			done = make(chan bool)
			doneLoading = false
			go func() {
				util.ShowSpinner(done)
			}()
		}

		var endpoint string
		var body interface{}

		if useResponsesAPI {
			endpoint = fmt.Sprintf("%s/v1/responses", httpEndpoint)
			input := messagesToInput(messages)
			body = NewResponsesRequestBody(model, input, true)
		} else {
			endpoint = fmt.Sprintf("%s/v1/chat/completions", httpEndpoint)
			body = NewChatRequestBody(messages, model, true, &StreamOptions{IncludeUsage: true})
			body, _ = ApplyChatOptions(body.(*ChatRequestBody), cmd)
		}

		jsonBody, err := json.Marshal(body)
		if err != nil {
			if useSpinner {
				done <- true
			}
			return messages, fmt.Errorf("marshaling request: %w", err)
		}

		req, err := http.NewRequest("POST", endpoint, bytes.NewReader(jsonBody))
		if err != nil {
			if useSpinner {
				done <- true
			}
			return messages, fmt.Errorf("creating request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
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
			if useSpinner {
				done <- true
			}
			return messages, fmt.Errorf("sending request: %w", err)
		}
		defer func() {
			if err := resp.Body.Close(); err != nil {
				slog.Error("closing response body", "error", err)
			}
		}()

		if resp.StatusCode != http.StatusOK {
			if useSpinner {
				done <- true
			}
			body, _ := io.ReadAll(resp.Body)
			return messages, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}

		// Get decompressing reader if response is compressed
		bodyReader := io.ReadCloser(resp.Body)
		contentEncoding := resp.Header.Get("Content-Encoding")
		if contentEncoding != "" {
			decompReader, err := getDecompressingReader(resp.Body, contentEncoding)
			if err != nil {
				if useSpinner {
					done <- true
				}
				return messages, fmt.Errorf("creating decompressor: %w", err)
			}
			bodyReader = decompReader
		}

		// Handle streaming response
		scanner := bufio.NewScanner(bodyReader)
		var responseMessage string

		for scanner.Scan() {
			chunk := scanner.Text()

			if !strings.HasPrefix(chunk, "data: ") {
				continue
			}
			chunk = strings.TrimPrefix(chunk, "data: ")

			if chunk == "[DONE]" {
				break
			}

			if useResponsesAPI {
				var streamEvent ResponseStreamEvent
				if err := json.Unmarshal([]byte(chunk), &streamEvent); err != nil {
					continue
				}

				if streamEvent.Type == "response.output_text.delta" {
					token := streamEvent.Delta
					if useSpinner && !doneLoading && token != "" {
						done <- true
						doneLoading = true
					}
					cmd.Printf("%s", token)
					responseMessage += token
				}

				if streamEvent.Type == "response.done" {
					break
				}
			} else {
				var chatResponse ChatCompletion
				if err := json.Unmarshal([]byte(chunk), &chatResponse); err != nil {
					continue
				}

				if useSpinner && !doneLoading {
					done <- true
					doneLoading = true
				}

				if len(chatResponse.Choices) > 0 {
					token := chatResponse.Choices[0].Delta.Content
					cmd.Printf("%s", token)
					responseMessage += token
				}
			}
		}

		if useSpinner && !doneLoading {
			done <- true
		}

		if responseMessage != "" {
			messages = append(messages, Message{Role: "assistant", Content: responseMessage})
		}
		cmd.Print("\n\n")

		return messages, nil
	}

	// Single message mode
	if len(args) > 0 {
		userMessage := args[0]
		messages := []Message{{Role: "user", Content: userMessage}}
		_, err := sendChatRequest(messages, false)
		if err != nil {
			slog.Error("chat request failed", "error", err)
			os.Exit(1)
		}
		return
	}

	// Interactive mode
	cmd.Printf("Welcome to the Spice.ai chat REPL! Type your message to chat with '%s'.\n", model)
	cmd.Println()

	var messages []Message
	line := liner.NewLiner()
	line.SetCtrlCAborts(true)
	defer func() {
		if err := line.Close(); err != nil {
			slog.Error("closing line", "error", err)
		}
	}()

	for {
		message, err := line.Prompt("chat> ")
		if err == liner.ErrPromptAborted {
			break
		} else if err == io.EOF {
			// EOF reached (Ctrl+D or piped input exhausted)
			break
		} else if err != nil {
			slog.Error("reading input line", "error", err)
			break
		}

		line.AppendHistory(message)
		messages = append(messages, Message{Role: "user", Content: message})

		messages, err = sendChatRequest(messages, true)
		if err != nil {
			slog.Error("chat request failed", "error", err)
			continue
		}
	}
}

func init() {
	chatCmd.Flags().String(constants.ModelKeyFlag, "", "Model to chat with")
	chatCmd.Flags().Float32("temperature", 1, "Model temperature for chat request")
	chatCmd.Flags().Bool("responses", false, "Whether to use the responses API for all completions")
	chatCmd.Flags().String("endpoint", "", "Specifies the remote Spice instance HTTP endpoint (e.g., http://localhost:8090)")
	chatCmd.Flags().StringSlice("headers", []string{}, "Custom HTTP headers to pass to remote endpoint in the format 'Key:Value'. Can be specified multiple times.")

	RootCmd.AddCommand(chatCmd)
}
