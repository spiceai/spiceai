package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var modelsCmd = &cobra.Command{
	Use:   "models",
	Short: "Lists models loaded by the Spice runtime",
	Example: `
spice models
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		err := api.WriteDataTable(rtcontext, "/v1/models", api.Model{})
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(modelsCmd)
}
