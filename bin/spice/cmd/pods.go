package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var podsCmd = &cobra.Command{
	Use:   "pods",
	Short: "Lists Spicepods loaded by the Spice runtime",
	Example: `
spice pods
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		err := api.WriteDataTable(rtcontext, "/v1/spicepods", api.Spicepod{})
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(podsCmd)
}
