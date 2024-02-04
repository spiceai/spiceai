package cmd

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/browser"
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
		authCode := generateAuthCode()

		cmd.Println("Opening browser to authenticate with Spice.ai")
		cmd.Printf("Auth Code: %s-%s\n", authCode[:4], authCode[4:])

		spiceApiClient := api.NewSpiceApiClient()
		err := spiceApiClient.Init()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		err = browser.OpenURL(spiceApiClient.GetAuthUrl(authCode))
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		var accessToken string

		cmd.Println("Waiting for authentication...")
		// poll for auth status
		for {
			time.Sleep(time.Second)

			authStatusResponse, err := spiceApiClient.ExchangeCode(authCode)
			if err != nil {
				cmd.Println("Error:", err)
				continue
			}

			if authStatusResponse.AccessToken != "" {
				accessToken = authStatusResponse.AccessToken
				break
			}
		}

		user, err := spiceApiClient.GetUser(accessToken)
		if err != nil {
			cmd.Println("Error:", err)
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_SPICE_AI, &api.Auth{
			Params: map[string]string{
				api.AUTH_PARAM_TOKEN: accessToken,
			},
		})

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully logged in to Spice.ai as %s (%s)", user.Username, user.Email)))
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
			cmd.Println("No password provided, use --password or -p to provide a password")
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_DREMIO, &api.Auth{
			Params: map[string]string{
				api.AUTH_PARAM_USERNAME: username,
				api.AUTH_PARAM_PASSWORD: password,
			},
		},
		)

		cmd.Println(aurora.BrightGreen("Successfully logged in to Dremio"))
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

func generateAuthCode() string {
	randomBytes := make([]byte, 6)

	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}

	authCode := base64.URLEncoding.EncodeToString(randomBytes)
	authCode = strings.ToUpper(authCode)[:8]

	return authCode
}
