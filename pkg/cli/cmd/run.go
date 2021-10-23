package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/cli/runtime"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Spice.ai - starts the Spice.ai runtime, installing if necessary",
	Example: `
spice run

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		err := runtime.Run(contextFlag, "")
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	runCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	runCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(runCmd)
}
