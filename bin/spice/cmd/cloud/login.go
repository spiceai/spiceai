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

package cloud

import (
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/logrusorgru/aurora"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

const (
	charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// LoginCmd authenticates with Spice Cloud
var LoginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to Spice Cloud",
	Long: `Login to Spice Cloud to manage your apps, deployments, and API keys.

This command opens your browser to authenticate with Spice Cloud.
Once authenticated, your credentials are stored locally and used for
subsequent 'spice cloud' commands.`,
	Example: `
spice cloud login
`,
	Run: func(cmd *cobra.Command, args []string) {
		authCode := generateAuthCode()

		spiceApiClient := api.NewSpiceApiClient()
		err := spiceApiClient.Init()
		if err != nil {
			slog.Error("Error initializing Spice.ai API client", "error", err)
			os.Exit(1)
		}

		spiceAuthUrl := spiceApiClient.GetAuthUrl(authCode)

		cmd.Println("Opening Spice Cloud authorization page in your default browser...")
		cmd.Printf("\nYour auth code:\n\n  %s-%s\n", authCode[:4], authCode[4:])
		cmd.Println("\nIf the browser does not open, visit the following URL manually:")
		cmd.Printf("\n  %s\n\n", spiceAuthUrl)

		_ = browser.OpenURL(spiceAuthUrl)

		var accessToken string

		cmd.Println("Waiting for authentication...")
		// poll for auth status with timeout
		timeout := time.After(5 * time.Minute)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				slog.Error("Authentication timed out. Please try again.")
				os.Exit(1)
			case <-ticker.C:
				authStatusResponse, err := spiceApiClient.ExchangeCode(authCode)
				if err != nil {
					continue
				}

				if authStatusResponse.AccessDenied {
					slog.Error("Access denied")
					os.Exit(1)
				}

				if authStatusResponse.AccessToken != "" {
					accessToken = authStatusResponse.AccessToken
					goto authenticated
				}
			}
		}

	authenticated:
		// Get auth context to display user info
		spiceAuthContext, err := spiceApiClient.GetAuthContext(accessToken, nil, nil)
		if err != nil {
			slog.Error("Error getting auth context", "error", err)
			os.Exit(1)
		}

		// Store the token
		saveAuthToken(accessToken, spiceAuthContext.App.ApiKey)

		cmd.Println()
		cmd.Println(aurora.BrightGreen(fmt.Sprintf("✓ Successfully logged in to Spice Cloud as %s (%s)", spiceAuthContext.Username, spiceAuthContext.Email)))
		cmd.Println()
		cmd.Println("You can now use 'spice cloud' commands to manage your apps and deployments.")
		cmd.Println()
		cmd.Println("Quick start:")
		cmd.Println("  spice cloud apps              - List your apps")
		cmd.Println("  spice cloud create app <name> - Create a new app")
		cmd.Println("  spice cloud deploy --app <org/app> - Deploy your app")
	},
}

// WhoamiCmd shows the current authenticated user
var WhoamiCmd = &cobra.Command{
	Use:   "whoami",
	Short: "Show current authenticated user",
	Long:  `Display information about the currently authenticated Spice Cloud user.`,
	Example: `
spice cloud whoami
`,
	Run: func(cmd *cobra.Command, args []string) {
		client := api.NewCloudClient()
		if err := client.Init(); err != nil {
			cmd.Println("Not logged in. Run 'spice cloud login' to authenticate.")
			os.Exit(1)
		}

		spiceApiClient := api.NewSpiceApiClient()
		if err := spiceApiClient.Init(); err != nil {
			slog.Error("Error initializing API client", "error", err)
			os.Exit(1)
		}

		// Get auth token from environment
		token := getAuthToken()
		if token == "" {
			cmd.Println("Not logged in. Run 'spice cloud login' to authenticate.")
			os.Exit(1)
		}

		spiceAuthContext, err := spiceApiClient.GetAuthContext(token, nil, nil)
		if err != nil {
			cmd.Println("Session expired or invalid. Run 'spice cloud login' to re-authenticate.")
			os.Exit(1)
		}

		cmd.Printf("Logged in as: %s (%s)\n", spiceAuthContext.Username, spiceAuthContext.Email)
		cmd.Printf("Organization: %s\n", spiceAuthContext.Org.Name)
		if spiceAuthContext.App.Name != "" {
			cmd.Printf("Default App:  %s/%s\n", spiceAuthContext.Org.Name, spiceAuthContext.App.Name)
		}
	},
}

// LogoutCmd logs out from Spice Cloud
var LogoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Logout from Spice Cloud",
	Long:  `Remove stored Spice Cloud credentials from your local environment.`,
	Example: `
spice cloud logout
`,
	Run: func(cmd *cobra.Command, args []string) {
		envFile := ".env"
		if _, err := os.Stat(".env.local"); err == nil {
			envFile = ".env.local"
		}

		spiceEnv, err := godotenv.Read(envFile)
		if err != nil {
			cmd.Println(aurora.BrightGreen("✓ Already logged out"))
			return
		}

		// Remove Spice.ai auth tokens
		delete(spiceEnv, "SPICE_SPICEAI_TOKEN")
		delete(spiceEnv, "SPICE_SPICEAI_API_KEY")

		if len(spiceEnv) == 0 {
			// If the env file is empty, just remove it
			if err := os.Remove(envFile); err != nil && !os.IsNotExist(err) {
				slog.Error("Error removing credentials file", "error", err)
				os.Exit(1)
			}
		} else {
			if err := godotenv.Write(spiceEnv, envFile); err != nil {
				slog.Error("Error updating credentials file", "error", err)
				os.Exit(1)
			}
		}

		cmd.Println(aurora.BrightGreen("✓ Successfully logged out from Spice Cloud"))
	},
}

func generateAuthCode() string {
	randomString := make([]byte, 8)
	charsetLength := big.NewInt(int64(len(charset)))

	for i := 0; i < 8; i++ {
		randomIndex, _ := rand.Int(rand.Reader, charsetLength)
		randomString[i] = charset[randomIndex.Int64()]
	}

	return string(randomString)
}

func saveAuthToken(token, apiKey string) {
	envFile := ".env"
	if _, err := os.Stat(".env.local"); err == nil {
		envFile = ".env.local"
	}

	spiceEnv, _ := godotenv.Read(envFile)
	// Ignore any errors reading the file - we will write a new one later

	spiceEnv["SPICE_SPICEAI_TOKEN"] = token
	if apiKey != "" {
		spiceEnv["SPICE_SPICEAI_API_KEY"] = apiKey
	}

	err := godotenv.Write(spiceEnv, envFile)
	if err != nil {
		slog.Error("Error saving credentials", "error", err)
		os.Exit(1)
	}

	if err := os.Chmod(envFile, 0600); err != nil {
		slog.Error("Error setting file permissions", "error", err)
		os.Exit(1)
	}
}

func getAuthToken() string {
	// First check environment variable
	if token := os.Getenv("SPICE_SPICEAI_TOKEN"); token != "" {
		return token
	}

	// Try .env.local first, then .env
	envFile := ".env"
	if _, err := os.Stat(".env.local"); err == nil {
		envFile = ".env.local"
	} else if _, err := os.Stat(envFile); err != nil {
		return ""
	}

	envValues, err := godotenv.Read(envFile)
	if err != nil {
		return ""
	}

	return envValues["SPICE_SPICEAI_TOKEN"]
}
