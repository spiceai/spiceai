/*
Copyright 2024 The Spice.ai OSS Authors

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
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v2"
)

const (
	apiKeyFlag       = "key"
	usernameFlag     = "username"
	passwordFlag     = "password"
	token            = "token"
	accessKeyFlag    = "access-key"
	accessSecretFlag = "access-secret"
	awsRegion        = "aws-region"
	awsAccessKeyId   = "aws-access-key-id"
	awsSecret        = "aws-secret-access-key"
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

		spiceApiClient := api.NewSpiceApiClient()
		err := spiceApiClient.Init()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		spiceAuthUrl := spiceApiClient.GetAuthUrl(authCode)

		cmd.Println("Attempting to open Spice.ai authorization page in your default browser")
		cmd.Printf("\nYour auth code:\n\n%s-%s\n", authCode[:4], authCode[4:])
		cmd.Println("\nIf the browser does not open, please visit the following URL manually:")
		cmd.Printf("\n%s\n\n", spiceAuthUrl)

		browser.OpenURL(spiceApiClient.GetAuthUrl(authCode))

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
			var spicePod spec.SpicepodSpec
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

var s3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Login to a s3 storage",
	Example: `
spice login s3 --access-key <key> --access-secret <secret>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_S3, map[string]string{
		accessKeyFlag:    fmt.Sprintf("No access key provided, use --%s or -k to provide a key", accessKeyFlag),
		accessSecretFlag: fmt.Sprintf("No access secret provided, use --%s or -s to provide a secret", accessSecretFlag),
	}, map[string]string{
		accessKeyFlag:    api.AUTH_PARAM_KEY,
		accessSecretFlag: api.AUTH_PARAM_SECRET,
	}),
}

var postgresCmd = &cobra.Command{
	Use:   "postgres",
	Short: "Login to a Postgres instance",
	Example: `
spice login postgres --password <password>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_POSTGRES, map[string]string{
		passwordFlag: fmt.Sprintf("No password provided, use --%s or -p to provide a password", passwordFlag),
	}, map[string]string{
		passwordFlag: api.AUTH_PARAM_PG_PASSWORD,
	}),
}

var postgresEngineCmd = &cobra.Command{
	Use:   "engine",
	Short: "Login to a Postgres instance as an engine",
	Example: `
spice login postgres engine --password <password>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_POSTGRES_ENGINE, map[string]string{
		passwordFlag: fmt.Sprintf("No password provided, use --%s or -p to provide a password", passwordFlag),
	}, map[string]string{
		passwordFlag: api.AUTH_PARAM_PG_PASSWORD,
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

var databricksCmd = &cobra.Command{
	Use:   "databricks",
	Short: "Login to a Databricks instance",
	Example: `
spice login databricks --token <access-token> --aws-region <aws-region> --aws-access-key-id <aws-access-key-id> --aws-secret-access-key <aws-secret-access-key>

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {

		token, err := cmd.Flags().GetString(token)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if token == "" {
			cmd.Println("No Databricks Access Token provided, use --token")
			os.Exit(1)
		}

		awsRegion, err := cmd.Flags().GetString(awsRegion)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if awsRegion == "" {
			cmd.Println("No AWS Region provided, use --aws-region")
			os.Exit(1)
		}

		awsAccessKeyId, err := cmd.Flags().GetString(awsAccessKeyId)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if awsAccessKeyId == "" {
			cmd.Println("No AWS Access Key ID provided, use --aws-access-key-id")
			os.Exit(1)
		}

		awsSecret, err := cmd.Flags().GetString(awsSecret)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if awsSecret == "" {
			cmd.Println("No AWS Secret Access Key provided, use --aws-secret-access-key")
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_DATABRICKS, &api.Auth{
			Params: map[string]string{
				api.AUTH_PARAM_TOKEN:                 token,
				api.AUTH_PARAM_AWS_DEFAULT_REGION:    awsRegion,
				api.AUTH_PARAM_AWS_ACCESS_KEY_ID:     awsAccessKeyId,
				api.AUTH_PARAM_AWS_SECRET_ACCESS_KEY: awsSecret,
			},
		},
		)

		cmd.Println(aurora.BrightGreen("Successfully logged in to Databricks"))
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

	databricksCmd.Flags().StringP(token, "p", "", "Access Token")
	databricksCmd.Flags().String(awsRegion, "", "AWS Region")
	databricksCmd.Flags().String(awsAccessKeyId, "", "AWS Access Key ID")
	databricksCmd.Flags().String(awsSecret, "", "AWS Secret Access Key")
	loginCmd.AddCommand(databricksCmd)

	s3Cmd.Flags().BoolP("help", "h", false, "Print this help message")
	s3Cmd.Flags().StringP(accessKeyFlag, "k", "", "Access key")
	s3Cmd.Flags().StringP(accessSecretFlag, "s", "", "Access Secret")
	loginCmd.AddCommand(s3Cmd)

	postgresCmd.Flags().BoolP("help", "h", false, "Print this help message")
	postgresCmd.Flags().StringP(passwordFlag, "p", "", "Password")
	loginCmd.AddCommand(postgresCmd)

	postgresEngineCmd.Flags().BoolP("help", "h", false, "Print this help message")
	postgresEngineCmd.Flags().StringP(passwordFlag, "p", "", "Password")
	postgresCmd.AddCommand(postgresEngineCmd)

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
