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

const (
	AUTH_TYPE_SPICE_AI   = "SPICEAI"
	AUTH_TYPE_DREMIO     = "DREMIO"
	AUTH_TYPE_S3         = "S3"
	AUTH_TYPE_DATABRICKS = "DATABRICKS"
	AUTH_TYPE_DELTA_LAKE = "DELTA_LAKE"
	AUTH_TYPE_POSTGRES   = "PG"
	AUTH_TYPE_SNOWFLAKE  = "SNOWFLAKE"
	AUTH_TYPE_SPARK      = "SPARK"
	AUTH_TYPE_SHAREPOINT = "SHAREPOINT"
	AUTH_TYPE_ABFS       = "ABFS"

	AUTH_PARAM_API_KEY                = "API_KEY"
	AUTH_PARAM_KEY                    = "KEY"
	AUTH_PARAM_ACCOUNT                = "ACCOUNT"
	AUTH_PARAM_PASS                   = "PASS"
	AUTH_PARAM_PASSWORD               = "PASSWORD"
	AUTH_PARAM_USERNAME               = "USERNAME"
	AUTH_PARAM_TOKEN                  = "TOKEN"
	AUTH_PARAM_BEARER_TOKEN           = "BEARER_TOKEN"
	AUTH_PARAM_AUTHORIZATION_CODE     = "AUTH_CODE"
	AUTH_PARAM_REMOTE                 = "REMOTE"
	AUTH_PARAM_PRIVATE_KEY_PATH       = "PRIVATE_KEY_PATH"
	AUTH_PARAM_PRIVATE_KEY_PASSPHRASE = "PRIVATE_KEY_PASSPHRASE"
	AUTH_PARAM_CLIENT_ID              = "CLIENT_ID"
	AUTH_PARAM_TENANT_ID              = "TENANT_ID"

	AUTH_PARAM_AWS_DEFAULT_REGION    = "AWS_DEFAULT_REGION"
	AUTH_PARAM_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AUTH_PARAM_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	AUTH_PARAM_SECRET                = "SECRET"

	AUTH_PARAM_AZURE_ACCOUNT_NAME = "AZURE_STORAGE_ACCOUNT_NAME"
	AUTH_PARAM_AZURE_ACCESS_KEY   = "AZURE_STORAGE_ACCESS_KEY"

	AUTH_PARAM_GCP_SERVICE_ACCOUNT_KEY_PATH = "GOOGLE_SERVICE_ACCOUNT_PATH"
)
