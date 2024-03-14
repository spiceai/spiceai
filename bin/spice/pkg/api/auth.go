package api

const (
	AUTH_TYPE_SPICE_AI = "spiceai"
	AUTH_TYPE_DREMIO   = "dremio"
	AUTH_TYPE_S3       = "s3"
	AUTH_TYPE_DATABRICKS = "databricks"

	AUTH_PARAM_KEY      = "key"
	AUTH_PARAM_PASSWORD = "password"
	AUTH_PARAM_USERNAME = "username"
	AUTH_PARAM_TOKEN    = "token"

	AUTH_PARAM_AWS_DEFAULT_REGION    = "AWS_DEFAULT_REGION"
	AUTH_PARAM_AWS_ACCESS_KEY_ID     = "AWS_ACCESS_KEY_ID"
	AUTH_PARAM_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
	AUTH_PARAM_SECRET   = "secret"
)

type Auth struct {
	Params map[string]string `json:"params,omitempty" csv:"params" toml:"params,omitempty"`
}
