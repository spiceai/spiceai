package api

const (
	AUTH_TYPE_SPICE_AI = "spice.ai"
	AUTH_TYPE_DREMIO   = "dremio"
)

type Auth struct {
	ProviderType string `json:"provider_type,omitempty" csv:"provider_type" toml:"provider_type,omitempty"`
	Key          string `json:"key,omitempty" csv:"key" toml:"key,omitempty"`
	Username     string `json:"username,omitempty" csv:"username" toml:"username,omitempty"`
	Password     string `json:"password,omitempty" csv:"password" toml:"password,omitempty"`
}
