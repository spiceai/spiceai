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
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spiceai/spiceai/bin/spice/pkg/version"
)

const (
	CloudAPIBaseURL    = "https://api.spice.ai"
	DevCloudAPIBaseURL = "https://dev-api.spice.ai"
)

// CloudClient is the client for the Spice Cloud Control API
type CloudClient struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// App represents a Spice Cloud app
type App struct {
	ID               int64      `json:"id,omitempty"`
	Name             string     `json:"name,omitempty"`
	Org              string     `json:"org,omitempty"`
	Description      string     `json:"description,omitempty"`
	Visibility       string     `json:"visibility,omitempty"`
	CreatedAt        string     `json:"created_at,omitempty"`
	Region           string     `json:"region,omitempty"`
	ProductionBranch string     `json:"production_branch,omitempty"`
	APIKey           string     `json:"api_key,omitempty"`
	Config           *AppConfig `json:"config,omitempty"`
}

// FullName returns the full app name in org/app format
func (a *App) FullName() string {
	if a.Org != "" {
		return fmt.Sprintf("%s/%s", a.Org, a.Name)
	}
	return a.Name
}

// AppConfig represents app configuration
type AppConfig struct {
	Spicepod           map[string]interface{} `json:"spicepod,omitempty"`
	ImageTag           string                 `json:"image_tag,omitempty"`
	Replicas           int                    `json:"replicas,omitempty"`
	NodeGroup          string                 `json:"node_group,omitempty"`
	StorageClaimSizeGB float64                `json:"storage_claim_size_gb,omitempty"`
}

// AppsResponse wraps the list of apps response
type AppsResponse struct {
	Apps []App `json:"apps,omitempty"`
}

// CreateAppRequest represents the request body for creating an app
type CreateAppRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Visibility  string `json:"visibility,omitempty"`
}

// UpdateAppRequest represents the request body for updating an app
type UpdateAppRequest struct {
	Description      string      `json:"description,omitempty"`
	Visibility       string      `json:"visibility,omitempty"`
	ProductionBranch string      `json:"production_branch,omitempty"`
	Spicepod         interface{} `json:"spicepod,omitempty"`
	ImageTag         string      `json:"image_tag,omitempty"`
	Replicas         int         `json:"replicas,omitempty"`
	NodeGroup        string      `json:"node_group,omitempty"`
	Region           string      `json:"region,omitempty"`
	StorageClaimSize float64     `json:"storage_claim_size_gb,omitempty"`
}

// Deployment represents a Spice Cloud deployment
type Deployment struct {
	ID             int64  `json:"id,omitempty"`
	Status         string `json:"status,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
	StartedAt      string `json:"started_at,omitempty"`
	FinishedAt     string `json:"finished_at,omitempty"`
	ImageTag       string `json:"image_tag,omitempty"`
	Replicas       int    `json:"replicas,omitempty"`
	CommitSHA      string `json:"commit_sha,omitempty"`
	CommitMessage  string `json:"commit_message,omitempty"`
	ErrorMessage   string `json:"error_message,omitempty"`
	CreationSource string `json:"creation_source,omitempty"`
}

// DeploymentsResponse wraps the list of deployments response
type DeploymentsResponse struct {
	Deployments []Deployment `json:"deployments,omitempty"`
}

// CreateDeploymentRequest represents the request body for creating a deployment
type CreateDeploymentRequest struct {
	ImageTag      string `json:"image_tag,omitempty"`
	Replicas      int    `json:"replicas,omitempty"`
	Branch        string `json:"branch,omitempty"`
	CommitSHA     string `json:"commit_sha,omitempty"`
	CommitMessage string `json:"commit_message,omitempty"`
	Debug         bool   `json:"debug,omitempty"`
}

// APIKeysResponse represents the API keys for an app
type APIKeysResponse struct {
	APIKey  *string `json:"api_key,omitempty"`
	APIKey2 *string `json:"api_key_2,omitempty"`
}

// RegenerateAPIKeyRequest represents the request body for regenerating an API key
type RegenerateAPIKeyRequest struct {
	KeyNumber int `json:"key_number,omitempty"`
}

// RegenerateAPIKeyResponse represents the response from regenerating an API key
type RegenerateAPIKeyResponse struct {
	APIKey         *string `json:"api_key,omitempty"`
	APIKey2        *string `json:"api_key_2,omitempty"`
	RegeneratedKey int     `json:"regenerated_key,omitempty"`
}

// Region represents a deployment region
type Region struct {
	Name         string `json:"name,omitempty"`
	Region       string `json:"region,omitempty"`
	Provider     string `json:"provider,omitempty"`
	ProviderName string `json:"providerName,omitempty"`
	IsDefault    bool   `json:"isDefault,omitempty"`
	Disabled     bool   `json:"disabled,omitempty"`
}

// RegionsResponse wraps the list of regions response
type RegionsResponse struct {
	Regions []Region `json:"regions,omitempty"`
	Default string   `json:"default,omitempty"`
}

// ContainerImage represents a container image
type ContainerImage struct {
	Name    string `json:"name,omitempty"`
	Tag     string `json:"tag,omitempty"`
	Channel string `json:"channel,omitempty"`
}

// ContainerImagesResponse wraps the list of container images response
type ContainerImagesResponse struct {
	Images  []ContainerImage `json:"images,omitempty"`
	Default string           `json:"default,omitempty"`
}

// NewCloudClient creates a new CloudClient
func NewCloudClient() *CloudClient {
	baseURL := CloudAPIBaseURL
	if strings.HasSuffix(version.Version(), "-dev") {
		baseURL = DevCloudAPIBaseURL
	}

	if envURL := os.Getenv("SPICE_CLOUD_API_URL"); envURL != "" {
		baseURL = envURL
	}

	return &CloudClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Init initializes the client with the auth token from the environment
func (c *CloudClient) Init() error {
	token := c.getAuthToken()
	if token == "" {
		return fmt.Errorf("not authenticated. Run 'spice login' to authenticate with Spice.ai")
	}
	c.token = token
	return nil
}

// getAuthToken retrieves the auth token from environment or .env files
func (c *CloudClient) getAuthToken() string {
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

// doRequest performs an HTTP request to the Cloud API
func (c *CloudClient) doRequest(method, path string, body interface{}) (*http.Response, error) {
	fullURL := fmt.Sprintf("%s%s", c.baseURL, path)

	var bodyReader io.Reader
	if body != nil {
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonBytes)
	}

	request, err := http.NewRequest(method, fullURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	return c.httpClient.Do(request)
}

// handleResponse processes the HTTP response and decodes into the target
func handleResponse[T any](resp *http.Response, target *T) error {
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("closing response body", "error", err)
		}
	}()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		if target != nil {
			if err := json.Unmarshal(bodyBytes, target); err != nil {
				return fmt.Errorf("failed to decode response: %w", err)
			}
		}
		return nil
	case http.StatusNoContent:
		return nil
	case http.StatusUnauthorized:
		return fmt.Errorf("unauthorized: invalid or expired token. Run 'spice login' to re-authenticate")
	case http.StatusForbidden:
		return fmt.Errorf("forbidden: insufficient permissions for this operation")
	case http.StatusNotFound:
		return fmt.Errorf("not found: the requested resource does not exist")
	case http.StatusConflict:
		return fmt.Errorf("conflict: %s", string(bodyBytes))
	case http.StatusBadRequest:
		return fmt.Errorf("bad request: %s", string(bodyBytes))
	default:
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(bodyBytes))
	}
}

// ListApps returns all apps for the authenticated organization
func (c *CloudClient) ListApps() ([]App, error) {
	resp, err := c.doRequest("GET", "/v1/apps", nil)
	if err != nil {
		return nil, err
	}

	var result AppsResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return result.Apps, nil
}

// GetAppByID returns details for a specific app by ID
func (c *CloudClient) GetAppByID(appID int64) (*App, error) {
	resp, err := c.doRequest("GET", fmt.Sprintf("/v1/apps/%d", appID), nil)
	if err != nil {
		return nil, err
	}

	var result App
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetApp returns details for a specific app by org/name
func (c *CloudClient) GetApp(orgApp string) (*App, error) {
	// List all apps and find the one matching org/name
	apps, err := c.ListApps()
	if err != nil {
		return nil, err
	}

	org, name := ParseOrgApp(orgApp)

	for i := range apps {
		app := &apps[i]
		if app.Name == name && (org == "" || app.Org == org) {
			// Fetch full details
			return c.GetAppByID(app.ID)
		}
	}

	return nil, fmt.Errorf("not found: app '%s' does not exist", orgApp)
}

// ParseOrgApp parses an org/app string into org and app components
func ParseOrgApp(orgApp string) (org, app string) {
	parts := strings.SplitN(orgApp, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

// CreateApp creates a new app
func (c *CloudClient) CreateApp(req *CreateAppRequest) (*App, error) {
	resp, err := c.doRequest("POST", "/v1/apps", req)
	if err != nil {
		return nil, err
	}

	var result App
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteAppByID soft deletes an app by ID
func (c *CloudClient) DeleteAppByID(appID int64) error {
	resp, err := c.doRequest("DELETE", fmt.Sprintf("/v1/apps/%d", appID), nil)
	if err != nil {
		return err
	}

	return handleResponse[any](resp, nil)
}

// DeleteApp soft deletes an app by org/name
func (c *CloudClient) DeleteApp(orgApp string) error {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return err
	}
	return c.DeleteAppByID(app.ID)
}

// UpdateAppByID updates an existing app by ID
func (c *CloudClient) UpdateAppByID(appID int64, req *UpdateAppRequest) (*App, error) {
	resp, err := c.doRequest("PUT", fmt.Sprintf("/v1/apps/%d", appID), req)
	if err != nil {
		return nil, err
	}

	var result App
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateApp updates an existing app by org/name
func (c *CloudClient) UpdateApp(orgApp string, req *UpdateAppRequest) (*App, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.UpdateAppByID(app.ID, req)
}

// ListDeploymentsByID returns deployments for an app by ID
func (c *CloudClient) ListDeploymentsByID(appID int64, limit int, status string) ([]Deployment, error) {
	path := fmt.Sprintf("/v1/apps/%d/deployments", appID)

	query := url.Values{}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if status != "" {
		query.Set("status", status)
	}
	if len(query) > 0 {
		path = fmt.Sprintf("%s?%s", path, query.Encode())
	}

	resp, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result DeploymentsResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return result.Deployments, nil
}

// ListDeployments returns deployments for an app by org/name
func (c *CloudClient) ListDeployments(orgApp string, limit int, status string) ([]Deployment, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.ListDeploymentsByID(app.ID, limit, status)
}

// CreateDeploymentByID creates a new deployment for an app by ID
func (c *CloudClient) CreateDeploymentByID(appID int64, req *CreateDeploymentRequest) (*Deployment, error) {
	resp, err := c.doRequest("POST", fmt.Sprintf("/v1/apps/%d/deployments", appID), req)
	if err != nil {
		return nil, err
	}

	var result Deployment
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateDeployment creates a new deployment for an app by org/name
func (c *CloudClient) CreateDeployment(orgApp string, req *CreateDeploymentRequest) (*Deployment, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.CreateDeploymentByID(app.ID, req)
}

// GetAPIKeysByID returns the API keys for an app by ID
func (c *CloudClient) GetAPIKeysByID(appID int64) (*APIKeysResponse, error) {
	resp, err := c.doRequest("GET", fmt.Sprintf("/v1/apps/%d/api-keys", appID), nil)
	if err != nil {
		return nil, err
	}

	var result APIKeysResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAPIKeys returns the API keys for an app by org/name
func (c *CloudClient) GetAPIKeys(orgApp string) (*APIKeysResponse, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.GetAPIKeysByID(app.ID)
}

// RegenerateAPIKeyByID regenerates an API key for an app by ID
func (c *CloudClient) RegenerateAPIKeyByID(appID int64, keyNumber int) (*RegenerateAPIKeyResponse, error) {
	req := &RegenerateAPIKeyRequest{KeyNumber: keyNumber}
	resp, err := c.doRequest("POST", fmt.Sprintf("/v1/apps/%d/api-keys", appID), req)
	if err != nil {
		return nil, err
	}

	var result RegenerateAPIKeyResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// RegenerateAPIKey regenerates an API key for an app by org/name
func (c *CloudClient) RegenerateAPIKey(orgApp string, keyNumber int) (*RegenerateAPIKeyResponse, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.RegenerateAPIKeyByID(app.ID, keyNumber)
}

// ListRegions returns available deployment regions
func (c *CloudClient) ListRegions(env string) (*RegionsResponse, error) {
	path := "/v1/regions"
	if env != "" {
		path = fmt.Sprintf("%s?env=%s", path, url.QueryEscape(env))
	}

	resp, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result RegionsResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListContainerImages returns available container images
func (c *CloudClient) ListContainerImages(channel string) (*ContainerImagesResponse, error) {
	path := "/v1/container-images"
	if channel != "" {
		path = fmt.Sprintf("%s?channel=%s", path, url.QueryEscape(channel))
	}

	resp, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result ContainerImagesResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Secret represents a secret for an app
type Secret struct {
	ID        int64  `json:"id,omitempty"`
	Name      string `json:"name"`
	Value     string `json:"value,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

// SecretsResponse wraps the list of secrets
type SecretsResponse struct {
	Secrets []Secret `json:"secrets,omitempty"`
}

// SetSecretRequest represents the request body for setting a secret
type SetSecretRequest struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// LogEntry represents a log entry from a deployment
type LogEntry struct {
	Timestamp string `json:"timestamp,omitempty"`
	Level     string `json:"level,omitempty"`
	Message   string `json:"message,omitempty"`
	Source    string `json:"source,omitempty"`
}

// LogsResponse wraps the list of logs
type LogsResponse struct {
	Logs []LogEntry `json:"logs,omitempty"`
}

// GetDeploymentByID returns a specific deployment by ID
func (c *CloudClient) GetDeploymentByID(appID, deploymentID int64) (*Deployment, error) {
	resp, err := c.doRequest("GET", fmt.Sprintf("/v1/apps/%d/deployments/%d", appID, deploymentID), nil)
	if err != nil {
		return nil, err
	}

	var result Deployment
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetDeployment returns a specific deployment by org/name and deployment ID
func (c *CloudClient) GetDeployment(orgApp string, deploymentID int64) (*Deployment, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.GetDeploymentByID(app.ID, deploymentID)
}

// GetLatestDeployment returns the most recent deployment for an app
func (c *CloudClient) GetLatestDeployment(orgApp string) (*Deployment, error) {
	deployments, err := c.ListDeployments(orgApp, 1, "")
	if err != nil {
		return nil, err
	}
	if len(deployments) == 0 {
		return nil, fmt.Errorf("not found: no deployments found for '%s'", orgApp)
	}
	return &deployments[0], nil
}

// GetDeploymentLogsByID returns logs for a deployment by app and deployment IDs
func (c *CloudClient) GetDeploymentLogsByID(appID, deploymentID int64, limit int, since string) (*LogsResponse, error) {
	path := fmt.Sprintf("/v1/apps/%d/deployments/%d/logs", appID, deploymentID)

	query := url.Values{}
	if limit > 0 {
		query.Set("limit", fmt.Sprintf("%d", limit))
	}
	if since != "" {
		query.Set("since", since)
	}
	if len(query) > 0 {
		path = fmt.Sprintf("%s?%s", path, query.Encode())
	}

	resp, err := c.doRequest("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var result LogsResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetDeploymentLogs returns logs for a deployment by org/name
func (c *CloudClient) GetDeploymentLogs(orgApp string, deploymentID int64, limit int, since string) (*LogsResponse, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.GetDeploymentLogsByID(app.ID, deploymentID, limit, since)
}

// ListSecretsByID returns secrets for an app by ID
func (c *CloudClient) ListSecretsByID(appID int64) ([]Secret, error) {
	resp, err := c.doRequest("GET", fmt.Sprintf("/v1/apps/%d/secrets", appID), nil)
	if err != nil {
		return nil, err
	}

	var result SecretsResponse
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return result.Secrets, nil
}

// ListSecrets returns secrets for an app by org/name
func (c *CloudClient) ListSecrets(orgApp string) ([]Secret, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.ListSecretsByID(app.ID)
}

// GetSecretByID returns a specific secret by app ID and name
func (c *CloudClient) GetSecretByID(appID int64, name string) (*Secret, error) {
	resp, err := c.doRequest("GET", fmt.Sprintf("/v1/apps/%d/secrets/%s", appID, url.PathEscape(name)), nil)
	if err != nil {
		return nil, err
	}

	var result Secret
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetSecret returns a specific secret by org/name and secret name
func (c *CloudClient) GetSecret(orgApp, name string) (*Secret, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.GetSecretByID(app.ID, name)
}

// SetSecretByID creates or updates a secret for an app by ID
func (c *CloudClient) SetSecretByID(appID int64, name, value string) (*Secret, error) {
	req := &SetSecretRequest{Name: name, Value: value}
	resp, err := c.doRequest("POST", fmt.Sprintf("/v1/apps/%d/secrets", appID), req)
	if err != nil {
		return nil, err
	}

	var result Secret
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// SetSecret creates or updates a secret for an app by org/name
func (c *CloudClient) SetSecret(orgApp, name, value string) (*Secret, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.SetSecretByID(app.ID, name, value)
}

// DeleteSecretByID deletes a secret for an app by ID
func (c *CloudClient) DeleteSecretByID(appID int64, name string) error {
	resp, err := c.doRequest("DELETE", fmt.Sprintf("/v1/apps/%d/secrets/%s", appID, url.PathEscape(name)), nil)
	if err != nil {
		return err
	}
	return handleResponse[any](resp, nil)
}

// DeleteSecret deletes a secret for an app by org/name
func (c *CloudClient) DeleteSecret(orgApp, name string) error {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return err
	}
	return c.DeleteSecretByID(app.ID, name)
}

// RollbackByID creates a rollback deployment to a specific deployment ID
func (c *CloudClient) RollbackByID(appID, targetDeploymentID int64) (*Deployment, error) {
	req := map[string]int64{"target_deployment_id": targetDeploymentID}
	resp, err := c.doRequest("POST", fmt.Sprintf("/v1/apps/%d/rollback", appID), req)
	if err != nil {
		return nil, err
	}

	var result Deployment
	if err := handleResponse(resp, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Rollback creates a rollback deployment for an app by org/name
func (c *CloudClient) Rollback(orgApp string, targetDeploymentID int64) (*Deployment, error) {
	app, err := c.GetApp(orgApp)
	if err != nil {
		return nil, err
	}
	return c.RollbackByID(app.ID, targetDeploymentID)
}
