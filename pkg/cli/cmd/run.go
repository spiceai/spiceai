package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/cli/runtime"
	"github.com/spiceai/spice/pkg/context"
)

var (
	baremetal bool
)

var RunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Spice, install if necessary.",
	Example: `
# Run Spice, install if necessary
spice run

# Run Spice in watch mode
spice run -w

# See more at: https://docs.spiceai.io/getting-started/
`,
	Run: func(cmd *cobra.Command, args []string) {
		var cliContext context.RuntimeContext = context.Docker
		if baremetal {
			cliContext = context.BareMetal
		}

		// Dependencies
		err := runtime.Init(cliContext)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = runtime.Run(cliContext)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	RunCmd.Flags().BoolVarP(&baremetal, "baremetal", "b", false, "Starts the runtime and AI Engine in a local process, not in Docker")
	RunCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(RunCmd)
}
