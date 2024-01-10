package cmd

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
)

var (
	contextFlag        string
	algorithmFlag      string
	numberEpisodesFlag int64
	loggers            []string
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
		RootCmd.Println(err.Error())
		os.Exit(1)
	}

	if err := RootCmd.Execute(); err != nil {
		RootCmd.Println(err)
		os.Exit(-1)
	}
}

func initConfig() {
	viper.SetEnvPrefix("spice")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
}
