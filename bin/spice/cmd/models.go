package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var modelsCmd = &cobra.Command{
	Use:   "models",
	Short: "Lists models loaded by the Spice runtime",
	Example: `
spice models
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		model_statues, _, err := api.GetComponentStatuses(PROM_ENDPOINT)
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		models, err := api.GetData[api.Model](rtcontext, "/v1/models?status=true")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		table := make([]interface{}, len(models))
		for i, model := range models {
			statusEnum, exists := model_statues[model.Name]
			if exists {
				model.Status = statusEnum.String()
			}
			table[i] = model
		}
		util.WriteTable(table)
	},
}

func init() {
	RootCmd.AddCommand(modelsCmd)
}
