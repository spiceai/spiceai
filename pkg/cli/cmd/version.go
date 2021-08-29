package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/context"
	"github.com/spiceai/spice/pkg/version"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Spice CLI version",
	Example: `
spice version
`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("CLI version:     %s\n", version.Version())

		var rtversion string
		var err error

		rtcontext, err := context.NewContext(contextFlag)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		err = rtcontext.Init()
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		if rtcontext.IsRuntimeInstallRequired() {
			rtversion = "not installed"
		} else {
			rtversion, err = rtcontext.Version()
			if err != nil {
				fmt.Printf("error getting runtime version: %s\n", err)
				os.Exit(1)
			}
		}

		fmt.Printf("Runtime version: %s\n", rtversion)
	},
}

func init() {
	versionCmd.Flags().StringVar(&contextFlag, "context", "docker", "Runs Spice.ai in the given context, either 'docker' or 'metal'")
	RootCmd.AddCommand(versionCmd)
}
