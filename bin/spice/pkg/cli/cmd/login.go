package cmd

import (
	"fmt"
	"os"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

const (
	apiKeyFlag = "key"
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to Spice.ai",
	Example: `
spice login

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {

		err := checkLatestCliReleaseVersion()
		if err != nil && util.IsDebug() {
			cmd.PrintErrf("failed to check for latest CLI release version: %s\n", err.Error())
		}

		key, err := cmd.Flags().GetString(apiKeyFlag)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if key == "" {
			cmd.Println("No API key provided, use --key or -k to provide an API key")
			os.Exit(1)
		}

		homeDir, err := os.UserHomeDir()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		authConfig := map[string]*api.Auth{
			api.AUTH_TYPE_SPICE_AI: {
				Key:          key,
				ProviderType: api.AUTH_TYPE_SPICE_AI,
			},
		}
		authConfigBytes, err := toml.Marshal(authConfig)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		spiceDir := fmt.Sprintf("%s/.spice", homeDir)
		err = os.MkdirAll(spiceDir, 0644)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		f, err := os.Create(fmt.Sprintf("%s/auth", spiceDir))
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		_, err = f.Write(authConfigBytes)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		cmd.Println(aurora.BrightGreen("Successfully logged in to Spice.ai"))
	},
}

func init() {
	loginCmd.Flags().BoolP("help", "h", false, "Print this help message")
	loginCmd.Flags().StringP(apiKeyFlag, "k", "", "API key")
	RootCmd.AddCommand(loginCmd)
}
