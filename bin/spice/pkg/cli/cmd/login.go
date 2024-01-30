package cmd

import (
	"fmt"
	"os"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

const (
	apiKeyFlag   = "key"
	usernameFlag = "username"
	passwordFlag = "password"
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to Spice.ai",
	Example: `
spice login

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		key, err := cmd.Flags().GetString(apiKeyFlag)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if key == "" {
			cmd.Println("No API key provided, use --key or -k to provide an API key")
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_SPICE_AI, &api.Auth{
			Key:          key,
			ProviderType: api.AUTH_TYPE_SPICE_AI,
		})

		cmd.Println(aurora.BrightGreen("Successfully logged in to Spice.ai"))
	},
}

var dremioCmd = &cobra.Command{
	Use:   "dremio",
	Short: "Login to a Dremio instance",
	Example: `
spice login dremio --username <username> --password <password>

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {

		username, err := cmd.Flags().GetString(usernameFlag)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if username == "" {
			cmd.Println("No username provided, use --username or -u to provide a username")
			os.Exit(1)
		}

		password, err := cmd.Flags().GetString(passwordFlag)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if password == "" {
			cmd.Println("No username provided, use --password or -p to provide a password")
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_DREMIO, &api.Auth{
			Password:     password,
			Username:     username,
			ProviderType: api.AUTH_TYPE_DREMIO,
		},
		)

		cmd.Println(aurora.BrightGreen("Successfully logged in to Spice.ai"))
	},
}

func mergeAuthConfig(cmd *cobra.Command, updatedAuthName string, updatedAuthConfig *api.Auth) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		cmd.Println(err.Error())
		os.Exit(1)
	}
	spiceDir := fmt.Sprintf("%s/.spice", homeDir)
	authFilePath := fmt.Sprintf("%s/auth", spiceDir)

	err = os.MkdirAll(spiceDir, 0644)
	if err != nil {
		cmd.Println(err.Error())
		os.Exit(1)
	}

	authConfig := map[string]*api.Auth{}
	if _, err := os.Stat(authFilePath); !os.IsNotExist(err) {
		authConfigBytes, err := os.ReadFile(authFilePath)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}

		err = toml.Unmarshal(authConfigBytes, &authConfig)
		if err != nil {
			cmd.Println(err)
			os.Exit(1)
		}
	}

	authConfig[updatedAuthName] = updatedAuthConfig
	updatedAuthConfigBytes, err := toml.Marshal(authConfig)
	if err != nil {
		cmd.Println(err.Error())
		os.Exit(1)
	}

	err = os.WriteFile(authFilePath, updatedAuthConfigBytes, 0644)
	if err != nil {
		cmd.Println(err.Error())
		os.Exit(1)
	}
}

func init() {
	dremioCmd.Flags().BoolP("help", "h", false, "Print this help message")
	dremioCmd.Flags().StringP(usernameFlag, "u", "", "Username")
	dremioCmd.Flags().StringP(passwordFlag, "p", "", "Password")
	loginCmd.AddCommand(dremioCmd)

	loginCmd.Flags().BoolP("help", "h", false, "Print this help message")
	loginCmd.Flags().StringP(apiKeyFlag, "k", "", "API key")
	RootCmd.AddCommand(loginCmd)
}
