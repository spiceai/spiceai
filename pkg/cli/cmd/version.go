package cmd

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Printf("CLI version:     %s\n", version.Version())

		var rtversion string
		var err error

		rtcontext, err := context.NewContext(contextFlag)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		err = rtcontext.Init(true)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if rtcontext.IsRuntimeInstallRequired() {
			rtversion = "not installed"
		} else {
			rtversion, err = rtcontext.Version()
			if err != nil {
				cmd.Printf("error getting runtime version: %s\n", err)
				os.Exit(1)
			}
		}

		cmd.Printf("Runtime version: %s\n", rtversion)
	},
}

func init() {
	versionCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(versionCmd)
}
