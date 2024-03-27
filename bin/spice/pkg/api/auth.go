/*
Copyright 2021-2024 The Spice Authors

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
	AUTH_TYPE_SPICE_AI        = "spiceai"
	AUTH_TYPE_DREMIO          = "dremio"
	AUTH_TYPE_S3              = "s3"
	AUTH_TYPE_DATABRICKS      = "databricks"
	AUTH_TYPE_POSTGRES        = "postgres"
	AUTH_TYPE_POSTGRES_ENGINE = "postgres_engine"
	AUTH_PARAM_PG_PASSWORD    = "pg_pass"

	AUTH_PARAM_KEY      = "key"
	AUTH_PARAM_PASSWORD = "password"
	AUTH_PARAM_USERNAME = "username"
	AUTH_PARAM_TOKEN    = "token"

	AUTH_PARAM_AWS_DEFAULT_REGION    = "AWS_DEFAULT_REGION"
	AUTH_PARAM_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AUTH_PARAM_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	AUTH_PARAM_SECRET                = "secret"
)

type Auth struct {
	Params map[string]string `json:"params,omitempty" csv:"params" toml:"params,omitempty"`
}
