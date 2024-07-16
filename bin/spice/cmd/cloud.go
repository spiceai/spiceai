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
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

var cloudCmd = &cobra.Command{
	Use:   "cloud",
	Short: "Spice.ai Cloud commands",
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequestBody struct {
	Messages []Message `json:"messages"`
}

type ChatRequesResponse struct {
	Content string `json:"content"`
	Role    string `json:"role"`
}

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Chat with the Spice.ai LLM agent",
	Example: `
...
`,
	Run: func(cmd *cobra.Command, args []string) {
		reader := bufio.NewReader(os.Stdin)

		spiceApiClient := api.NewSpiceApiClient()
		err := spiceApiClient.Init()
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		spiceBaseUrl := spiceApiClient.GetBaseUrl()
		githubToken := os.Getenv("GITHUB_TOKEN")

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

			url := fmt.Sprintf("%s/api/integrations/github/copilot/agent-callback", spiceBaseUrl)
			body := ChatRequestBody{Messages: messages}
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

			request.Header.Set("X-GitHub-Token", githubToken)
			response, err := client.Do(request)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			var chatResponse ChatRequesResponse
			err = json.NewDecoder(response.Body).Decode(&chatResponse)
			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}

			message = strings.TrimSpace(chatResponse.Content)
			if strings.ToLower(message) == "exit" {
				fmt.Println("Goodbye!")
				break
			}

			done <- true

			for _, char := range message {
				cmd.Printf("%c", char)
			}

			messages = append(messages, Message{Role: "assistant", Content: message})

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
	cloudCmd.AddCommand(chatCmd)
	RootCmd.AddCommand(cloudCmd)
}
