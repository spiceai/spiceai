package api

const (
	AUTH_TYPE_SPICE_AI = "spice.ai"
	AUTH_TYPE_DREMIO   = "dremio"

	AUTH_PARAM_KEY      = "key"
	AUTH_PARAM_PASSWORD = "password"
	AUTH_PARAM_USERNAME = "username"
)

type Auth struct {
	Params map[string]string `json:"params,omitempty" csv:"params" toml:"params,omitempty"`
}
