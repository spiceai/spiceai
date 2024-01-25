package api

const (
	AUTH_TYPE_SPICE_AI = "spice.ai"
)

type AuthConfig struct {
	Auth *Auth `json:"auth,omitempty" csv:"auth" toml:"auth"`
}

type Auth struct {
	ProviderType string `json:"provider_type,omitempty" csv:"provider_type" toml:"provider_type"`
	Key          string `json:"key,omitempty" csv:"key" toml:"key"`
}
