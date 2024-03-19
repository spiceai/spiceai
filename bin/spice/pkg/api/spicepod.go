package api

type Spicepod struct {
	Version           string `json:"version,omitempty" csv:"version" yaml:"version,omitempty"`
	Name              string `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	DatasetsCount     int    `json:"datasets_count,omitempty" csv:"datasets_count" yaml:"datasets_count,omitempty"`
	ModelsCount       int    `json:"models_count,omitempty" csv:"models_count" yaml:"models_count,omitempty"`
	DependenciesCount int    `json:"dependencies_count,omitempty" csv:"dependencies_count" yaml:"dependencies_count,omitempty"`
}
