package spec

type SpicepodSpec struct {
	Version      string            `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Kind         string            `json:"kind,omitempty" csv:"kind" yaml:"kind,omitempty"`
	Name         string            `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	Params       map[string]string `json:"params,omitempty" yaml:"params,omitempty" mapstructure:"params,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty" csv:"metadata" yaml:"metadata,omitempty"`
	Datasets     []*Reference      `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Functions    []*Reference      `json:"functions,omitempty" csv:"functions" yaml:"functions,omitempty"`
	Models       []*Reference      `json:"models,omitempty" csv:"models" yaml:"models,omitempty"`
	Dependencies []string          `json:"dependencies,omitempty" csv:"dependencies" yaml:"dependencies,omitempty"`
	Secrets      Secrets           `json:"secrets,omitempty" csv:"secrets" yaml:"secrets,omitempty"`
}

type Reference struct {
	Ref       string `json:"ref,omitempty" csv:"ref" yaml:"ref,omitempty"`
	DependsOn string `json:"depends_on,omitempty" csv:"depends_on" yaml:"dependsOn,omitempty"`
}

type Secrets struct {
	Store string `json:"store,omitempty" csv:"store" yaml:"store,omitempty"`
}
