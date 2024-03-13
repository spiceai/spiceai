package api

const (
	AUTH_TYPE_SPICE_AI = "spiceai"
	AUTH_TYPE_DREMIO   = "dremio"
	AUTH_TYPE_S3       = "s3"

	AUTH_PARAM_KEY      = "key"
	AUTH_PARAM_PASSWORD = "password"
	AUTH_PARAM_USERNAME = "username"
	AUTH_PARAM_TOKEN    = "token"
	AUTH_PARAM_SECRET   = "secret"
)

type Auth struct {
	Params map[string]string `json:"params,omitempty" csv:"params" toml:"params,omitempty"`
}
