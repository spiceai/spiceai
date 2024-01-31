package api

const (
	AUTH_TYPE_SPICE_AI = "spice.ai"
	AUTH_TYPE_DREMIO   = "dremio"
)

type Auth struct {
	Key      string `json:"key,omitempty" csv:"key" toml:"key,omitempty"`
	Username string `json:"username,omitempty" csv:"username" toml:"username,omitempty"`
	Password string `json:"password,omitempty" csv:"password" toml:"password,omitempty"`
}
