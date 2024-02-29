package cmd

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"gopkg.in/yaml.v2"
)

const (
	apiKeyFlag       = "key"
	usernameFlag     = "username"
	passwordFlag     = "password"
	accessKeyFlag    = "access-key"
	accessSecretFlag = "access-scret"
	charset          = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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

			if authStatusResponse.AccessDenied {
				cmd.Println("Access denied")
				os.Exit(1)
			}

			if authStatusResponse.AccessToken != "" {
				accessToken = authStatusResponse.AccessToken
				break
			}
		}

		// try reading spicepod.yaml, to check if we have preferred org and app
		var orgName string
		var appName string
		spicepodBytes, err := os.ReadFile("spicepod.yaml")
		if err == nil {
			var spicePod api.Pod
			err = yaml.Unmarshal(spicepodBytes, &spicePod)
			if err == nil {
				if spicePod.Metadata != nil {
					orgName = spicePod.Metadata["org"]
				}
				appName = spicePod.Name
			}
		}

		spiceAuthContext, err := spiceApiClient.GetAuthContext(accessToken, &orgName, &appName)
		if err != nil {
			cmd.Println("Error:", err)
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_SPICE_AI, &api.Auth{
			Params: map[string]string{
				api.AUTH_PARAM_TOKEN: accessToken,
				api.AUTH_PARAM_KEY:   spiceAuthContext.App.ApiKey,
			},
		})

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully logged in to Spice.ai as %s (%s)", spiceAuthContext.Username, spiceAuthContext.Email)))
		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Using app %s/%s", spiceAuthContext.Org.Name, spiceAuthContext.App.Name)))
	},
}

var dremioCmd = &cobra.Command{
	Use:   "dremio",
	Short: "Login to a Dremio instance",
	Example: `
spice login dremio --username <username> --password <password>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_DREMIO, map[string]string{
		usernameFlag: fmt.Sprintf("No username provided, use --%s or -u to provide a username", usernameFlag),
		passwordFlag: fmt.Sprintf("No password provided, use --%s or -p to provide a password", passwordFlag),
	}, map[string]string{
		usernameFlag: api.AUTH_PARAM_USERNAME,
		passwordFlag: api.AUTH_PARAM_PASSWORD,
	}),
}

var minioCmd = &cobra.Command{
	Use:   "minio",
	Short: "Login to a minio storage",
	Example: `
spice login minio --access-key <key> --access-secret <secret>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_MINIO, map[string]string{
		accessKeyFlag:    fmt.Sprintf("No access key provided, use --%s or -k to provide a key", accessKeyFlag),
		accessSecretFlag: fmt.Sprintf("No access secret provided, use --%s or -s to provide a secret", accessSecretFlag),
	}, map[string]string{
		accessKeyFlag:    api.AUTH_PARAM_KEY,
		accessSecretFlag: api.AUTH_PARAM_SECRET,
	}),
}

func CreateLoginRunFunc(authName string, requiredFlags map[string]string, flagToTomlKeys map[string]string) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {

		authParams := make(map[string]string)
		for flag, errMsg := range requiredFlags {
			value, err := cmd.Flags().GetString(flag)
			if err != nil {
				cmd.Println(err.Error())
				os.Exit(1)
			}
			if value == "" {
				cmd.Println(errMsg)
				os.Exit(1)
			}
			authParams[flag] = value
		}

		// Convert keys from user flags to those to write to authConfig. Default to flag key.
		configParams := make(map[string]string, len(authParams))
		for k, v := range authParams {
			if newK, exists := flagToTomlKeys[k]; exists {
				configParams[newK] = v
			} else {
				configParams[k] = v
			}
		}
		mergeAuthConfig(cmd, authName, &api.Auth{
			Params: configParams,
		})

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully logged in to %s", authName)))
	}
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

	minioCmd.Flags().BoolP("help", "h", false, "Print this help message")
	minioCmd.Flags().StringP(accessKeyFlag, "k", "", "Access key")
	minioCmd.Flags().StringP(accessSecretFlag, "s", "", "Access Secret")
	loginCmd.AddCommand(minioCmd)

	loginCmd.Flags().BoolP("help", "h", false, "Print this help message")
	loginCmd.Flags().StringP(apiKeyFlag, "k", "", "API key")
	RootCmd.AddCommand(loginCmd)
}

func generateAuthCode() string {
	randomString := make([]byte, 8)
	charsetLength := big.NewInt(int64(len(charset)))

	for i := 0; i < 8; i++ {
		randomIndex, _ := rand.Int(rand.Reader, charsetLength)
		randomString[i] = charset[randomIndex.Int64()]
	}

	authCode := string(randomString)

	return authCode
}
