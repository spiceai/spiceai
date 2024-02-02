package cmd

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/logrusorgru/aurora"
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/browser"
	"github.com/spf13/cobra"
	"github.com/spiceai/spiceai/bin/spice/pkg/api"
)

const (
	usernameFlag = "username"
	passwordFlag = "password"
)

type CloudAuth struct {
	ProviderToken        string `json:"provider_token"`
	ProviderRefreshToken string `json:"provider_refresh_token"`
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to Spice.ai",
	Example: `
spice login

# See more at: https://docs.spiceai.org/
`,
	Run: func(cmd *cobra.Command, args []string) {

		err := browser.OpenURL(fmt.Sprintf("https://cloud-git-mitch-device-auth-spice.vercel.app/login?cli-callback=true"))
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		auth, err := listenAndGetAuth()
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		fmt.Println(auth)

		req, err := http.NewRequest("GET", "https://cloud-git-mitch-device-auth-spice.vercel.app/api/orgs", nil)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		req.AddCookie(&http.Cookie{
			Name:    "gh_token",
			Value:   auth.ProviderToken,
			Expires: time.Now().Add(time.Hour * 7),
		})
		req.AddCookie(&http.Cookie{
			Name:    "gh_refresh_token",
			Value:   auth.ProviderRefreshToken,
			Expires: time.Now().Add(time.Hour * 24 * 180),
		})

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			cmd.Println(err.Error())
			os.Exit(1)
		}

		if resp.StatusCode != 200 {
			cmd.Println("Failed to retrieve orgs: " + string(body))
			os.Exit(1)
		}

		//println(string(body))

		// mergeAuthConfig(cmd, api.AUTH_TYPE_SPICE_AI, &api.Auth{
		// 	Params: map[string]string{
		// 		api.AUTH_PARAM_KEY:      key,
		// 		api.AUTH_PARAM_PASSWORD: key,
		// 	},
		// })

		cmd.Println(aurora.BrightGreen("Successfully logged in to Spice.ai"))
	},
}

func listenAndGetAuth() (*CloudAuth, error) {
	authChan := make(chan *CloudAuth)

	// TODO: Make this a pretty web page that matches our branding. Also move focus back to terminal.
	http.HandleFunc("/auth/callback", func(w http.ResponseWriter, r *http.Request) {
		fmt.Print(r)

		token, err := r.Cookie("gh_token")
		if err != nil {
			fmt.Printf("Authorization failed. Did not receive token.")
			authChan <- &CloudAuth{}
			return
		}

		refreshToken, err := r.Cookie("gh_refresh_token")
		if err != nil {
			fmt.Printf("Authorization failed. Did not receive refresh token.")
			authChan <- &CloudAuth{}
			return
		}

		fmt.Println("Authorization successful. You can now return to the CLI.")
		authChan <- &CloudAuth{
			ProviderToken:        token.Value,
			ProviderRefreshToken: refreshToken.Value,
		}
	})

	go func() {
		if err := http.ListenAndServe(":3000", nil); err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()

	auth := <-authChan
	return auth, nil
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
	RootCmd.AddCommand(loginCmd)
}
