package api

const (
	AUTH_TYPE_SPICE_AI = "spice.ai"
)

type AuthConfig struct {
	Auth *Auth `json:"auth,omitempty" csv:"auth" toml:"auth"`
}

type Auth struct {
	Type string `json:"type,omitempty" csv:"type" toml:"type"`
	Key  string `json:"key,omitempty" csv:"key" toml:"key"`
}
