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
	"unicode"

	"github.com/joho/godotenv"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
	"github.com/spiceai/spiceai/bin/spice/pkg/msal"
	"github.com/spiceai/spiceai/bin/spice/pkg/spec"
	"gopkg.in/yaml.v3"
)

const (
	apiKeyFlag            = "key"
	accountFlag           = "account"
	usernameFlag          = "username"
	passwordFlag          = "password"
	privateKeyPathFlag    = "private-key-path"
	passphraseFlag        = "passphrase"
	token                 = "token"
	accessKeyFlag         = "access-key"
	accessSecretFlag      = "access-secret"
	sparkRemoteFlag       = "spark_remote"
	awsRegion             = "aws-region"
	awsAccessKeyId        = "aws-access-key-id"
	awsSecret             = "aws-secret-access-key"
	azureAccountName      = "azure-storage-account-name"
	azureAccessKey        = "azure-storage-access-key"
	gcpServiceAccountPath = "google-service-account-path"
	m365TenantId          = "tenant-id"
	m365ClientId          = "client-id"
	charset               = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
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

		_ = browser.OpenURL(spiceApiClient.GetAuthUrl(authCode))

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

		mergeAuthConfig(cmd, api.AUTH_TYPE_SPICE_AI, map[string]string{
			api.AUTH_PARAM_TOKEN:   accessToken,
			api.AUTH_PARAM_API_KEY: spiceAuthContext.App.ApiKey,
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
	}, []string{}),
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
	}, []string{}),
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
		passwordFlag: api.AUTH_PARAM_PASS,
	}, []string{}),
}

var snowflakeCmd = &cobra.Command{
	Use:   "snowflake",
	Short: "Login to a Snowflake warehouse",
	Example: `
spice login snowflake --account <account-identifier> --username <username> --password <password>
spice login snowflake --account <account-identifier> --username <username> --private-key-path <private-key-path> --passphrase <passphrase>

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {

		if privateKeyPath, err := cmd.Flags().GetString(privateKeyPathFlag); err == nil && privateKeyPath != "" {

			CreateLoginRunFunc(api.AUTH_TYPE_SNOWFLAKE, map[string]string{
				accountFlag:        fmt.Sprintf("No account identifier provided, use --%s or -a to provide an account identifier", accountFlag),
				usernameFlag:       fmt.Sprintf("No username provided, use --%s or -u to provide a username", usernameFlag),
				privateKeyPathFlag: fmt.Sprintf("No private key path provided, use --%s or -k to provide a private key path", privateKeyPathFlag),
			}, map[string]string{
				accountFlag:        api.AUTH_PARAM_ACCOUNT,
				usernameFlag:       api.AUTH_PARAM_USERNAME,
				privateKeyPathFlag: api.AUTH_PARAM_PRIVATE_KEY_PATH,
				passphraseFlag:     api.AUTH_PARAM_PRIVATE_KEY_PASSPHRASE,
			}, []string{
				passphraseFlag,
			})(cmd, args)

			return
		}

		// default username/password login
		CreateLoginRunFunc(api.AUTH_TYPE_SNOWFLAKE, map[string]string{
			accountFlag:  fmt.Sprintf("No account identifier provided, use --%s or -a to provide an account identifier", accountFlag),
			usernameFlag: fmt.Sprintf("No username provided, use --%s or -u to provide a username", usernameFlag),
			passwordFlag: fmt.Sprintf("No password provided, use --%s or -p to provide a password", passwordFlag),
		}, map[string]string{
			accountFlag:  api.AUTH_PARAM_ACCOUNT,
			usernameFlag: api.AUTH_PARAM_USERNAME,
			passwordFlag: api.AUTH_PARAM_PASSWORD,
		}, []string{})(cmd, args)

	},
}

/*
*
AuthorizationCodeCredential::authorization_url_builder(client_id)

	.with_redirect_uri(redirect_uri)
	.with_scope(scope)
	.with_tenant(tenant_id)
	.url()
*/
var m365Cmd = &cobra.Command{
	Use:   "m365",
	Short: "Login to a Microsoft 365 account",
	Example: `
	spice login m365 --username <username> --password <password> --tenant-id <tenant-id> --client-id <client-id>

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		tenant_id, err := cmd.Flags().GetString(m365TenantId)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		} else if tenant_id == "" {
			cmd.Println("No tenant_id provided, use --tenant-id")
			os.Exit(1)
		}

		client_id, err := cmd.Flags().GetString(m365ClientId)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		} else if client_id == "" {
			cmd.Println("No client_id provided, use --client-id")
			os.Exit(1)
		}

		auth_code, err := msal.GetAuthCode(cmd.Context(), tenant_id, client_id, []string{"User.Read", "Files.Read"})
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_SHAREPOINT, map[string]string{
			api.AUTH_PARAM_AUTHORISATION_CODE: auth_code,
		})
	},
}

var sparkCmd = &cobra.Command{
	Use:   "spark",
	Short: "Login to a Spark Connect remote",
	Example: `
spice login spark --spark_remote <remote>

# See more at: https://docs.spiceai.org/
`,
	Run: CreateLoginRunFunc(api.AUTH_TYPE_SPARK, map[string]string{
		sparkRemoteFlag: "No spark_remote provided, use --spark_remote to provide",
	}, map[string]string{
		sparkRemoteFlag: api.AUTH_PARAM_REMOTE,
	}, []string{}),
}

func CreateLoginRunFunc(authName string, requiredFlags map[string]string, flagToEnvKeys map[string]string, optionalFlags []string) func(cmd *cobra.Command, args []string) {
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

		for _, flag := range optionalFlags {
			value, err := cmd.Flags().GetString(flag)
			if err != nil {
				cmd.Println(err.Error())
				os.Exit(1)
			}
			if value != "" {
				authParams[flag] = value
			}
		}

		// Convert keys from user flags to those to write to authConfig. Default to flag key.
		configParams := make(map[string]string, len(authParams))
		for k, v := range authParams {
			if newK, exists := flagToEnvKeys[k]; exists {
				configParams[newK] = v
			} else {
				configParams[k] = v
			}
		}
		mergeAuthConfig(cmd, authName, configParams)

		cmd.Println(aurora.BrightGreen(fmt.Sprintf("Successfully logged in to %s", toSentenceCase(authName))))
	}
}

func toSentenceCase(s string) string {
	if len(s) == 0 {
		return ""
	}
	runes := []rune(s)
	runes[0] = unicode.ToUpper(runes[0])
	for i := 1; i < len(runes); i++ {
		runes[i] = unicode.ToLower(runes[i])
	}
	return string(runes)
}

var databricksCmd = &cobra.Command{
	Use:   "databricks",
	Short: "Login to a Databricks instance",
	Example: `
# Using Spark Connect
spice login databricks --token <access-token>

# Using Delta Lake directly against AWS S3
spice login databricks --token <access-token> --aws-region <aws-region> --aws-access-key-id <aws-access-key-id> --aws-secret-access-key <aws-secret-access-key>

# Using Delta Lake directly against Azure Blob Storage
spice login databricks --token <access-token> --azure-storage-account-name <account-name> --azure-storage-access-key <access-key>

# Using Delta Lake directly against Google Cloud Storage
spice login databricks --token <access-token> --google-service-account-path /path/to/service-account.json

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		db_token, err := cmd.Flags().GetString(token)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if db_token == "" {
			cmd.Println("No Databricks Access Token provided, use --token")
			os.Exit(1)
		}

		params := map[string]string{
			api.AUTH_PARAM_TOKEN: db_token,
		}

		awsRegion, err := cmd.Flags().GetString(awsRegion)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsRegion != "" {
			params[api.AUTH_PARAM_AWS_DEFAULT_REGION] = awsRegion
		}

		awsAccessKeyId, err := cmd.Flags().GetString(awsAccessKeyId)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsAccessKeyId != "" {
			params[api.AUTH_PARAM_AWS_ACCESS_KEY_ID] = awsAccessKeyId
		}

		awsSecret, err := cmd.Flags().GetString(awsSecret)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsSecret != "" {
			params[api.AUTH_PARAM_AWS_SECRET_ACCESS_KEY] = awsSecret
		}

		azureAccountName, err := cmd.Flags().GetString(azureAccountName)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if azureAccountName != "" {
			params[api.AUTH_PARAM_AZURE_ACCOUNT_NAME] = azureAccountName
		}

		azureAccessKey, err := cmd.Flags().GetString(azureAccessKey)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if azureAccessKey != "" {
			params[api.AUTH_PARAM_AZURE_ACCESS_KEY] = azureAccessKey
		}

		gcpServiceAccountPath, err := cmd.Flags().GetString(gcpServiceAccountPath)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if gcpServiceAccountPath != "" {
			params[api.AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH] = gcpServiceAccountPath
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_DATABRICKS, params)

		cmd.Println(aurora.BrightGreen("Successfully logged in to Databricks"))
	},
}

var deltaLakeCmd = &cobra.Command{
	Use:   "delta_lake",
	Short: "Configure credentials to access a Delta Lake table",
	Example: `
# Using Delta Lake with AWS S3
spice login delta_lake --aws-region <aws-region> --aws-access-key-id <aws-access-key-id> --aws-secret-access-key <aws-secret-access-key>

# Using Delta Lake with Azure Blob Storage
spice login delta_lake --azure-storage-account-name <account-name> --azure-storage-access-key <access-key>

# Using Delta Lake with Google Cloud Storage
spice login delta_lake --google-service-account-path /path/to/service-account.json

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {
		params := map[string]string{}

		awsRegion, err := cmd.Flags().GetString(awsRegion)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsRegion != "" {
			params[api.AUTH_PARAM_AWS_DEFAULT_REGION] = awsRegion
		}

		awsAccessKeyId, err := cmd.Flags().GetString(awsAccessKeyId)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsAccessKeyId != "" {
			params[api.AUTH_PARAM_AWS_ACCESS_KEY_ID] = awsAccessKeyId
		}

		awsSecret, err := cmd.Flags().GetString(awsSecret)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if awsSecret != "" {
			params[api.AUTH_PARAM_AWS_SECRET_ACCESS_KEY] = awsSecret
		}

		azureAccountName, err := cmd.Flags().GetString(azureAccountName)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if azureAccountName != "" {
			params[api.AUTH_PARAM_AZURE_ACCOUNT_NAME] = azureAccountName
		}

		azureAccessKey, err := cmd.Flags().GetString(azureAccessKey)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if azureAccessKey != "" {
			params[api.AUTH_PARAM_AZURE_ACCESS_KEY] = azureAccessKey
		}

		gcpServiceAccountPath, err := cmd.Flags().GetString(gcpServiceAccountPath)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		if gcpServiceAccountPath != "" {
			params[api.AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH] = gcpServiceAccountPath
		}

		mergeAuthConfig(cmd, api.AUTH_TYPE_DELTA_LAKE, params)

		cmd.Println(aurora.BrightGreen("Successfully configured credentials for Delta Lake"))
	},
}

func mergeAuthConfig(cmd *cobra.Command, updatedAuthName string, updatedAuthConfig map[string]string) {
	spiceEnv, _ := godotenv.Read(".env")
	// Ignore any errors reading the file - we will write a new one later

	for k, v := range updatedAuthConfig {
		secretKey := fmt.Sprintf("SPICE_%s_%s", updatedAuthName, k)
		spiceEnv[secretKey] = v
	}

	err := godotenv.Write(spiceEnv, ".env")
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
	databricksCmd.Flags().String(azureAccountName, "", "Azure Storage Account Name")
	databricksCmd.Flags().String(azureAccessKey, "", "Azure Storage Access Key")
	databricksCmd.Flags().String(gcpServiceAccountPath, "", "Google Service Account Path")
	loginCmd.AddCommand(databricksCmd)

	deltaLakeCmd.Flags().String(awsRegion, "", "AWS Region")
	deltaLakeCmd.Flags().String(awsAccessKeyId, "", "AWS Access Key ID")
	deltaLakeCmd.Flags().String(awsSecret, "", "AWS Secret Access Key")
	deltaLakeCmd.Flags().String(azureAccountName, "", "Azure Storage Account Name")
	deltaLakeCmd.Flags().String(azureAccessKey, "", "Azure Storage Access Key")
	deltaLakeCmd.Flags().String(gcpServiceAccountPath, "", "Google Service Account Path")
	loginCmd.AddCommand(deltaLakeCmd)

	m365Cmd.Flags().BoolP("help", "h", false, "Print this help message")
	m365Cmd.Flags().StringP(m365TenantId, "t", "", "Microsoft organization tenant ID")
	m365Cmd.Flags().StringP(m365ClientId, "c", "", "Microsoft Azure AD application client ID")
	loginCmd.AddCommand(m365Cmd)

	s3Cmd.Flags().BoolP("help", "h", false, "Print this help message")
	s3Cmd.Flags().StringP(accessKeyFlag, "k", "", "Access key")
	s3Cmd.Flags().StringP(accessSecretFlag, "s", "", "Access Secret")
	loginCmd.AddCommand(s3Cmd)

	postgresCmd.Flags().BoolP("help", "h", false, "Print this help message")
	postgresCmd.Flags().StringP(passwordFlag, "p", "", "Password")
	loginCmd.AddCommand(postgresCmd)

	snowflakeCmd.Flags().BoolP("help", "h", false, "Print this help message")
	snowflakeCmd.Flags().StringP(accountFlag, "a", "", "Account identifier")
	snowflakeCmd.Flags().StringP(usernameFlag, "u", "", "Username")
	snowflakeCmd.Flags().StringP(passwordFlag, "p", "", "Password")
	snowflakeCmd.Flags().StringP(privateKeyPathFlag, "k", "", "Private key path")
	snowflakeCmd.Flags().StringP(passphraseFlag, "s", "", "Passphrase")
	loginCmd.AddCommand(snowflakeCmd)

	sparkCmd.Flags().BoolP("help", "h", false, "Print this help message")
	sparkCmd.Flags().String(sparkRemoteFlag, "", "Spark Remote")
	loginCmd.AddCommand(sparkCmd)

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
