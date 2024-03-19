package api

import (
	"fmt"
	"strings"
)

type Model struct {
	Name     string   `json:"name,omitempty" csv:"name" yaml:"name,omitempty"`
	From     string   `json:"from,omitempty" csv:"from" yaml:"from,omitempty"`
	Datasets []string `json:"datasets,omitempty" csv:"datasets" yaml:"datasets,omitempty"`
}

func (m *Model) CellDisplay() string {
	datasets := strings.Join(m.Datasets, ", ")
	if len(datasets) == 0 {
		datasets = "None"
	}
	return fmt.Sprintf("Name: %s\nFrom: %s\nDatasets: %v\n", m.Name, m.From, datasets)
}
