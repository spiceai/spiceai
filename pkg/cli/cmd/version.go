package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version.Version())
	},
}

func init() {
	RootCmd.AddCommand(versionCmd)
}
