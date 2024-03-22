package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var datasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "Lists datasets loaded by the Spice runtime",
	Example: `
spice datasets
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		_, dataset_statues, err := api.GetComponentStatuses(PROM_ENDPOINT)
		if err != nil {
			cmd.PrintErrln(err.Error())
		}

		datasets, err := api.GetData[api.Dataset](rtcontext, "/v1/datasets?status=true")
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
		table := make([]interface{}, len(datasets))
		for i, dataset := range datasets {
			statusEnum, exists := dataset_statues[dataset.Name]
			if exists {
				dataset.Status = statusEnum.String()
			}
			table[i] = dataset
		}
		util.WriteTable(table)
	},
}

func init() {
	RootCmd.AddCommand(datasetsCmd)
}
