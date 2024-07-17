package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequestBody struct {
	Messages []Message `json:"messages"`
	Model    string    `json:"model"`
	Stream   bool      `json:"stream"`
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
	ID                string      `json:"id"`
	Choices           []Choice    `json:"choices"`
	Created           int64       `json:"created"`
	Model             string      `json:"model"`
	SystemFingerprint string      `json:"system_fingerprint"`
	Object            string      `json:"object"`
	Usage             interface{} `json:"usage"`
}

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Chat with the Spice.ai LLM agent",
	Example: `
...
`,
	Run: func(cmd *cobra.Command, args []string) {
		reader := bufio.NewReader(os.Stdin)

		spiceBaseUrl := os.Getenv("SPICE_BASE_URL")
		apiKey := os.Getenv("SPICE_API_KEY")

		client := &http.Client{}

		var messages []Message = []Message{}

		for {
			cmd.Print(">>> ")

			message, err := reader.ReadString('\n')
			if err != nil {
				cmd.Println(err.Error())
				os.Exit(1)
			}

			messages = append(messages, Message{Role: "user", Content: message})

			done := make(chan bool)
			go func() {
				spinner(done)
			}()

			url := fmt.Sprintf("%s/v1/chat/completions", spiceBaseUrl)
			body := ChatRequestBody{
				Messages: messages,
				Model:    "openai",
				Stream:   true,
			}
			jsonBody, err := json.Marshal(body)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			request, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			request.Header.Set("X-API-Key", apiKey)
			response, err := client.Do(request)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}
			done <- true

			scanner := bufio.NewScanner(response.Body)
			var responseMessage = ""

			for scanner.Scan() {
				chunk := scanner.Text()
				if !strings.HasPrefix(chunk, "data: ") {
					continue
				}
				chunk = strings.TrimPrefix(chunk, "data: ")

				var chatResponse ChatCompletion = ChatCompletion{}
				err = json.Unmarshal([]byte(chunk), &chatResponse)
				if err != nil {
					cmd.Println(err)
					continue
				}

				token := chatResponse.Choices[0].Delta.Content
				cmd.Printf("%s", token)
				responseMessage = responseMessage + token
			}

			messages = append(messages, Message{Role: "assistant", Content: responseMessage})

			cmd.Print("\n\n")
		}
	},
}

func spinner(done chan bool) {
	chars := []rune{'⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'}
	for {
		for _, char := range chars {
			select {
			case <-done:
				fmt.Print("\r") // Clear the spinner
				return
			default:
				fmt.Printf("\r%c ", char)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}

func init() {
	RootCmd.AddCommand(chatCmd)
}
