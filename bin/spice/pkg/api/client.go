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

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

type SpiceAuthContext struct {
	Email    string   `json:"email,omitempty"`
	Username string   `json:"username,omitempty"`
	Org      SpiceOrg `json:"org,omitempty"`
	App      SpiceApp `json:"app,omitempty"`
}

type SpiceOrg struct {
	Id   int64  `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

type SpiceApp struct {
	Id     int64  `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	ApiKey string `json:"api_key,omitempty"`
}

type AccessTokenResponse struct {
	AccessDenied bool   `json:"access_denied,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
}

type SpiceApiClient struct {
	baseUrl string
}

func NewSpiceApiClient() *SpiceApiClient {
	return &SpiceApiClient{}
}

func (s *SpiceApiClient) Init() error {
	if strings.HasSuffix(version.Version(), "-dev") {
		s.baseUrl = "https://dev.spice.ai"
	} else {
		s.baseUrl = "https://spice.ai"
	}

	if os.Getenv("SPICE_BASE_URL") != "" {
		s.baseUrl = os.Getenv("SPICE_BASE_URL")
	}

	return nil
}

func (s *SpiceApiClient) GetBaseUrl() string {
	return s.baseUrl
}

func (s *SpiceApiClient) GetAuthUrl(authCode string) string {
	return fmt.Sprintf("%s/auth/token?code=%s", s.baseUrl, authCode)
}

func (s *SpiceApiClient) GetAuthContext(accessToken string, orgName *string, appName *string) (SpiceAuthContext, error) {
	var spiceAuthContext SpiceAuthContext

	url := fmt.Sprintf("%s/api/spice-cli/auth?org_name=%s&app_name=%s", s.baseUrl, *orgName, *appName)

	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return spiceAuthContext, err
	}

	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return spiceAuthContext, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return spiceAuthContext, err
	}

	err = json.Unmarshal(body, &spiceAuthContext)

	if err != nil {
		return spiceAuthContext, err
	}

	return spiceAuthContext, nil
}

func (s *SpiceApiClient) ExchangeCode(authCode string) (AccessTokenResponse, error) {
	var authStatusResponse AccessTokenResponse

	payload := map[string]interface{}{
		"code": authCode,
	}

	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return authStatusResponse, err
	}

	request, err := http.NewRequest("POST", fmt.Sprintf("%s/auth/token/exchange", s.baseUrl), bytes.NewReader(jsonBody))
	if err != nil {
		return authStatusResponse, err
	}
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return authStatusResponse, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return authStatusResponse, err
	}

	err = json.Unmarshal(body, &authStatusResponse)

	if err != nil {
		return authStatusResponse, err
	}

	return authStatusResponse, nil
}
