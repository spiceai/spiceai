package spec

type DataspaceSpec struct {
	From    string            `json:"from,omitempty" yaml:"from,omitempty" mapstructure:"from,omitempty"`
	Name    string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Data    *DataSpec         `json:"data,omitempty" yaml:"data,omitempty" mapstructure:"data,omitempty"`
	Fields  []FieldSpec       `json:"fields,omitempty" yaml:"fields,omitempty" mapstructure:"fields,omitempty"`
	Actions map[string]string `json:"actions,omitempty" yaml:"actions,omitempty" mapstructure:"actions,omitempty"`
	Laws    []string          `json:"laws,omitempty" yaml:"laws,omitempty" mapstructure:"laws,omitempty"`
}

type DataSpec struct {
	Connector DataConnectorSpec `json:"connector,omitempty" yaml:"connector,omitempty" mapstructure:"connector,omitempty"`
	Processor DataProcessorSpec `json:"processor,omitempty" yaml:"processor,omitempty" mapstructure:"processor,omitempty"`
}

type FieldSpec struct {
	Name string `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Type string `json:"type,omitempty" yaml:"type,omitempty" mapstructure:"type,omitempty"`
	// Initializer needs to be a *float64 in order to properly handle zero values - "omitempty" will drop them otherwise
	Initializer *float64 `json:"initializer,omitempty" yaml:"initializer,omitempty" mapstructure:"initializer,omitempty"`
	Values      []string `json:"values,omitempty" yaml:"values,omitempty" mapstructure:"values,omitempty"`
	Fill        string   `json:"fill,omitempty" yaml:"fill,omitempty" mapstructure:"fill,omitempty"`
}
