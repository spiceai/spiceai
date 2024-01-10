package spec

type DataProcessorSpec struct {
	Name   string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Params map[string]string `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
}
