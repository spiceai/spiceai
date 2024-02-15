package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Start an interactive SQL query session against the Spice.ai runtime",
	Example: `
spice query
`,
	Run: func(cmd *cobra.Command, args []string) {
		rtcontext := context.NewContext()
		execCmd, err := rtcontext.GetRunCmd()
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		execCmd.Args = append(execCmd.Args, "--repl")

		execCmd.Stderr = os.Stderr
		execCmd.Stdout = os.Stdout
		execCmd.Stdin = os.Stdin

		err = util.RunCommand(execCmd)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	queryCmd.Flags().BoolP("help", "h", false, "Print this help message")
	RootCmd.AddCommand(queryCmd)
}
