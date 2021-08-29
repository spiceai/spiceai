package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/cli/runtime"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Spice.ai - starts the Spice.ai runtime, installing if necessary",
	Example: `
spice run

# See more at: https://spiceai.github.io/docs
`,
	Run: func(cmd *cobra.Command, args []string) {
		err := runtime.Run(contextFlag, "")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	runCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	runCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(runCmd)
}
