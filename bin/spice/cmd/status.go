package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Spice runtime status",
	Example: `
spice status
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		err := api.WriteDataTable(rtcontext, "/v1/status", api.Service{})
		if err != nil {
			cmd.PrintErrln(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(statusCmd)
}
