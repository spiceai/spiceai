package api

type AuthConfig struct {
	Auth *Auth `json:"auth,omitempty" csv:"auth" toml:"auth"`
}

type Auth struct {
	Key string `json:"key,omitempty" csv:"key" toml:"key"`
}
