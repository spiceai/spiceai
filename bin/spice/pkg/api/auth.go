package api

const (
	AUTH_TYPE_SPICE_AI = "spiceai"
	AUTH_TYPE_DREMIO   = "dremio"

	AUTH_PARAM_KEY      = "key"
	AUTH_PARAM_PASSWORD = "password"
	AUTH_PARAM_USERNAME = "username"
	AUTH_PARAM_TOKEN    = "token"
)

type Auth struct {
	Params map[string]string `json:"params,omitempty" csv:"params" toml:"params,omitempty"`
}
