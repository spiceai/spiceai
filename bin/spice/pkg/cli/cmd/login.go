package cmd

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/valyala/fasthttp"
	"gopkg.in/yaml.v2"
)

const (
	apiKeyFlag   = "key"
	usernameFlag = "username"
	passwordFlag = "password"
	charset      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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

		if !dremioAuth(username, password) {
			cmd.Println("Invalid dremio username or password")
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

func dremioAuth(username string, password string) bool {

	type userCredentials struct {
		UserName string `json:"userName"`
		Password string `json:"password"`
	}

	// TODO: Allow user to specify their own dremio host. Needs UX design for how the command should handle.
	// TODO: Do we ultimately want to use dremio token instead of username/password in the following dremio requests?
	const dremioHost = "dremio-4mimamg7rdeve.eastus.cloudapp.azure.com:9047"
	dremioLoginAddr := fmt.Sprintf("http://%s/apiv2/login", dremioHost)

	creds := userCredentials{
		UserName: username,
		Password: password,
	}

	jsonData, err := json.Marshal(creds)
	if err != nil {
		fmt.Println("Error marshaling JSON:", err)
	}

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(dremioLoginAddr)
	req.Header.SetMethod("POST")
	req.Header.SetContentType("application/json")
	req.SetBody(jsonData)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	err = fasthttp.Do(req, resp)
	if err != nil {
		fmt.Println("Error sending request:", err)
	}

	if resp.StatusCode() == fasthttp.StatusOK {
		return true
	}

	return false
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
	randomString := make([]byte, 8)
	charsetLength := big.NewInt(int64(len(charset)))

	for i := 0; i < 8; i++ {
		randomIndex, _ := rand.Int(rand.Reader, charsetLength)
		randomString[i] = charset[randomIndex.Int64()]
	}

	authCode := string(randomString)

	return authCode
}
