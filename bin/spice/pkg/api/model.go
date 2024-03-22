package api

type Model struct {
	Name     string   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	From     string   `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Datasets []string `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
	Status   string   `json:"status,omitempty" csv:"status,omitempty" yaml:"status,omitempty"`
}
