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

package util

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func IsRuntimeServerHealthy(serverBaseUrl string, httpClient *http.Client) error {
	url := fmt.Sprintf("%s/health", serverBaseUrl)
	resp, err := httpClient.Get(url)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil || strings.Trim(string(body), "\n") != "ok" {
		return errors.New(string(body))
	}

	return nil
}

func IsRuntimeServerReady(serverBaseUrl string, httpClient *http.Client, apiKey string) error {
	url := fmt.Sprintf("%s/v1/ready", serverBaseUrl)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != 200 {
		return errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil || strings.Trim(string(body), "\n") != "ready" {
		return errors.New(string(body))
	}

	return nil
}

// CheckRemoteServerHealth checks both /health and /v1/ready endpoints and warns if either fails.
// Returns the total duration of checks and whether both checks passed.
func CheckRemoteServerHealth(serverBaseUrl string, httpClient *http.Client, apiKey string) (time.Duration, bool) {
	startTime := time.Now()
	allPassed := true

	// Check /health endpoint
	if err := IsRuntimeServerHealthy(serverBaseUrl, httpClient); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Health check failed for %s/health: %v\n", serverBaseUrl, err)
		allPassed = false
	}

	// Check /v1/ready endpoint
	if err := IsRuntimeServerReady(serverBaseUrl, httpClient, apiKey); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Readiness check failed for %s/v1/ready: %v\n", serverBaseUrl, err)
		allPassed = false
	}

	duration := time.Since(startTime)
	return duration, allPassed
}
