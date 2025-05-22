/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

var verbosity = util.NewVerbosity()

var RootCmd = &cobra.Command{
	Use:   "spice",
	Short: "Spice.ai CLI",

	PersistentPreRun: func(cmd *cobra.Command, args []string) {

		if cmd.Name() == "version" {
			// don't duplicate version information in version command
			return
		}
		cmd.Printf("Spice.ai OSS CLI %s\n", version.Version())
	},
}

// Execute adds all child commands to the root command.
func Execute() {
	cobra.OnInitialize(initConfig)

	if err := RootCmd.Execute(); err != nil {
		slog.Error("Error executing command", "error", err)
		os.Exit(-1)
	}
}

func init() {
	RootCmd.PersistentFlags().CountVarP(&verbosity.VerbosityCount, "verbose", "v", "Verbose logging")
	RootCmd.PersistentFlags().BoolVar(&verbosity.VeryVerbose, "very-verbose", false, "Very verbose logging")
	RootCmd.PersistentFlags().BoolP("help", "h", false, "Print this help message")
	RootCmd.PersistentFlags().String("api-key", "", "The API key to use for authentication")
	RootCmd.PersistentFlags().Bool(constants.CloudKeyFlag, false, fmt.Sprintf("Use cloud instance of Spice. Requires `--%s", constants.ApiKeyFlag))
	RootCmd.PersistentFlags().String(constants.HttpEndpointKeyFlag, "http://localhost:8090", "HTTP endpoint of Spice")
	RootCmd.PersistentFlags().String(constants.UserAgentKeyFlag, util.GetSpiceUserAgent("spice"), "The user agent to use for all HTTP requests")
	RootCmd.PersistentFlags().String(constants.TlsRootCertificateFile, "", "The path to the root certificate file used to verify the Spice.ai runtime server certificate")
}

func initConfig() {
	viper.SetEnvPrefix("spice")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	initLogLevel()
}

func initLogLevel() {
	switch verbosity.GetLevel() {
	case 0:
		// Default `spice` with no flags
		slog.SetLogLoggerLevel(slog.LevelInfo)
	case 1:
		// `spice -v`, `spice --verbose`
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case 2:
		// `spice -vv`, `spice --very-verbose`
		slog.SetLogLoggerLevel(util.TRACE_LEVEL)
	}
}
