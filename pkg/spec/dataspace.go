package spec

type DataspaceSpec struct {
	From         string            `json:"from,omitempty" yaml:"from,omitempty" mapstructure:"from,omitempty"`
	Name         string            `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Data         *DataSpec         `json:"data,omitempty" yaml:"data,omitempty" mapstructure:"data,omitempty"`
	Time         *TimeSpec         `json:"time,omitempty" yaml:"time,omitempty" mapstructure:"time,omitempty"`
	Measurements []MeasurementSpec `json:"measurements,omitempty" yaml:"measurements,omitempty" mapstructure:"measurements,omitempty"`
	Categories   []CategorySpec    `json:"categories,omitempty" yaml:"categories,omitempty" mapstructure:"categories,omitempty"`
	Tags         []string          `json:"tags,omitempty" yaml:"tags,omitempty" mapstructure:"tags,omitempty"`
	Actions      map[string]string `json:"actions,omitempty" yaml:"actions,omitempty" mapstructure:"actions,omitempty"`
	Laws         []string          `json:"laws,omitempty" yaml:"laws,omitempty" mapstructure:"laws,omitempty"`
}

type DataSpec struct {
	Connector DataConnectorSpec `json:"connector,omitempty" yaml:"connector,omitempty" mapstructure:"connector,omitempty"`
	Processor DataProcessorSpec `json:"processor,omitempty" yaml:"processor,omitempty" mapstructure:"processor,omitempty"`
}

type TimeSpec struct {
	Selector string `json:"selector,omitempty" yaml:"selector,omitempty" mapstructure:"selector,omitempty"`
}

type MeasurementSpec struct {
	Name     string `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Selector string `json:"selector,omitempty" yaml:"selector,omitempty" mapstructure:"selector,omitempty"`
	// Initializer needs to be a *float64 in order to properly handle zero values - "omitempty" will drop them otherwise
	Initializer *float64 `json:"initializer,omitempty" yaml:"initializer,omitempty" mapstructure:"initializer,omitempty"`
	Fill        string   `json:"fill,omitempty" yaml:"fill,omitempty" mapstructure:"fill,omitempty"`
}

type CategorySpec struct {
	Name     string   `json:"name,omitempty" yaml:"name,omitempty" mapstructure:"name,omitempty"`
	Selector string   `json:"selector,omitempty" yaml:"selector,omitempty" mapstructure:"selector,omitempty"`
	Values   []string `json:"values,omitempty" yaml:"values,omitempty" mapstructure:"values,omitempty"`
}
