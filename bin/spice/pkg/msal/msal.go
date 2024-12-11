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
package msal

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/AzureAD/microsoft-authentication-library-for-go/apps/public"
)

// A function that triggers the user's browser to be directed to an interactive OAuth2.0 authorization.
// The user will be prompted to login and authorize the application to access the requested scopes.
// The function will block until the user has completed the authorization and the authorization code has been received. It is intended to be used in a CLI environment where the user can be directed to a browser.
//
// This function will temporarily start a local server on `:8091`.
func InteractivelyAccessToken(ctx context.Context, tenantId string, clientId string, scopes []string) (string, error) {
	authorityURI := fmt.Sprintf("https://login.microsoftonline.com/%s", tenantId)
	publicClient, err := public.New(clientId, public.WithAuthority(authorityURI))

	accounts, err := publicClient.Accounts(ctx)
	if err != nil {
		return "", fmt.Errorf("error creating public client: %w", err)
	}
	var result public.AuthResult
	if len(accounts) > 0 {
		result, err = publicClient.AcquireTokenSilent(ctx, scopes, public.WithSilentAccount(accounts[0]))
	}
	if err != nil || len(accounts) == 0 {
		result, err = publicClient.AcquireTokenInteractive(ctx, scopes, public.WithRedirectURI("http://localhost"))
		if err != nil {
			fmt.Errorf("error creating auth code URL: %w", err)
		}
	}
	access_token := result.AccessToken

	return access_token, nil
}

func run_redirect_server(output_chan chan string) {
	server := &http.Server{Addr: ":8091"}

	http.HandleFunc("/", construct_get_token(output_chan))

	if err := server.ListenAndServe(); err != nil {
		slog.Error("fatal error on server", "error", err)
		os.Exit(1)
	}
}

func construct_get_token(output chan string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		codes, ok := r.URL.Query()["code"]
		if !ok || len(codes[0]) < 1 {
			slog.Error("Authorization code missing")
			os.Exit(1)
		}

		code := codes[0]
		output <- code
		fmt.Fprintf(w, "Authorised!")
	}
}
