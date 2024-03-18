package api

type Service struct {
	Name     string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Endpoint string `json:"endpoint,omitempty" csv:"endpoint" yaml:"endpoint,omitempty"`
	Status   string `json:"status,omitempty" csv:"status" yaml:"status,omitempty"`
}
