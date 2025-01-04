package cmd

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

type EvalRequest struct {
	Model string `json:"model"`
}

type EvalResponse struct {
	ID        string             `json:"id"`
	CreatedAt string             `json:"created_at"`
	Dataset   string             `json:"dataset"`
	Model     string             `json:"model"`
	Status    string             `json:"status"`
	Scorers   []string           `json:"scorers"`
	Metrics   map[string]float64 `json:"metrics"`
}

type EvalResult struct {
	Input  string `json:"input"`
	Output string `json:"output"`
	Actual string `json:"actual"`
}

var evalCmd = &cobra.Command{
	Use:   "eval [eval-name]",
	Short: "Run model evaluation",
	Example: `
spice eval tetris --model "my_model"`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			slog.Error("eval name is required")
			return
		}
		evalName := args[0]

		model, err := cmd.Flags().GetString("model")
		if err != nil || model == "" {
			slog.Error("model is required")
			return
		}

		request := EvalRequest{Model: model}
		body, err := json.Marshal(request)
		if err != nil {
			slog.Error("marshaling request", "error", err)
			return
		}

		postBody := string(body)

		rtcontext := context.NewContext()

		url := fmt.Sprintf("/v1/evals/%s", evalName)
		response, err := api.PostRuntime[[]EvalResponse](rtcontext, url, &postBody)
		if err != nil {
			slog.Error("running evaluation", "error", err)
			return
		}

		table := make([]interface{}, len(response))
		for i, r := range response {
			table[i] = r
		}
		util.WriteTable(table)
	},
}

func init() {
	evalCmd.Flags().String("model", "", "Model to evaluate")

	RootCmd.AddCommand(evalCmd)
}
