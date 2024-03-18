package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var datasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "Lists datasets loaded by the Spice runtime",
	Example: `
spice datasets
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		err := api.WriteDataTable(rtcontext, "/v1/datasets", api.Dataset{})
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(datasetsCmd)
}
