package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

type SpiceUser struct {
	Email       string   `json:"email,omitempty"`
	Username    string   `json:"username,omitempty"`
	PersonalOrg SpiceOrg `json:"personal_org,omitempty"`
	App         SpiceApp `json:"app,omitempty"`
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
	if version.Version() == "local-dev" {
		s.baseUrl = "https://dev.spice.xyz"
	} else {
		s.baseUrl = "https://spice.ai"
	}

	if os.Getenv("SPICE_BASE_URL") != "" {
		s.baseUrl = os.Getenv("SPICE_BASE_URL")
	}

	return nil
}

func (s *SpiceApiClient) GetAuthUrl(authCode string) string {
	return fmt.Sprintf("%s/auth/token?code=%s", s.baseUrl, authCode)
}

func (s *SpiceApiClient) GetUser(accessToken string) (SpiceUser, error) {
	var spiceUser SpiceUser

	request, err := http.NewRequest("GET", fmt.Sprintf("%s/api/spice-cli/user", s.baseUrl), nil)
	if err != nil {
		return spiceUser, err
	}

	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return spiceUser, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return spiceUser, err
	}

	err = json.Unmarshal(body, &spiceUser)

	if err != nil {
		return spiceUser, err
	}

	return spiceUser, err
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
