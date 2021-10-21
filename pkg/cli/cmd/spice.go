package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/context"
)

var (
	contextFlag        string
	algorithmFlag      string
	numberEpisodesFlag int64
)

var RootCmd = &cobra.Command{
	Use:   "spice",
	Short: "Spice.ai CLI",
}

// Execute adds all child commands to the root command.
func Execute() {
	cobra.OnInitialize(initConfig)

	// All CLI commands run in the "metal" context
	err := context.SetDefaultContext()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func initConfig() {
	viper.SetEnvPrefix("spice")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}
