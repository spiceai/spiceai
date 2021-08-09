package spec

type ConnectorSpec struct {
	Type   string            `json:"type,omitempty" yaml:"type,omitempty" mapstructure:"type,omitempty"`
	Params map[string]string `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
}
