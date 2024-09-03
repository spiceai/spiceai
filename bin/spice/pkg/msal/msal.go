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
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/AzureAD/microsoft-authentication-library-for-go/apps/public"
	"github.com/pkg/browser"
)

func GetAuthCode(ctx context.Context, tenantId string, clientId string, scopes []string) (string, error) {
	publicClient, err := public.New(clientId)
	if err != nil {
		return "", fmt.Errorf("error creating public client: %w", err)
	}
	auth_url, err := publicClient.AuthCodeURL(ctx, clientId, "http://localhost:8091", scopes, public.WithTenantID(tenantId))
	if err != nil {
		return "", fmt.Errorf("error creating auth code URL: %w", err)
	}

	auth_code := make(chan string, 1)
	server_shutdown := make(chan struct{})

	// Start a local server to listen for the redirect
	go func() {
		run_redirect_server(auth_code, server_shutdown)
	}()

	fmt.Println("Attempting to open Microsoft authorization page in your default browser")
	fmt.Println("\nIf the browser does not open, please visit the following URL manually:")
	fmt.Printf("\n%s\n\n", auth_url)
	_ = browser.OpenURL(auth_url)

	// Wait for the auth code
	code := <-auth_code

	// Signal the server to shutdown
	close(server_shutdown)

	return code, nil
}

func run_redirect_server(output_chan chan string, shutdown_chan chan struct{}) {
	server := &http.Server{Addr: ":8091"}

	http.HandleFunc("/", construct_get_token(output_chan))

	go func() {
		<-shutdown_chan
		if err := server.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func construct_get_token(output chan string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		codes, ok := r.URL.Query()["code"]
		if !ok || len(codes[0]) < 1 {
			log.Fatal(errors.New("Authorization code missing"))
		}

		code := codes[0]
		output <- code
		fmt.Fprintf(w, "Authorised!")
	}
}
