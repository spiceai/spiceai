package observation

import (
	_ "embed"

	spice_time "github.com/spiceai/spiceai/pkg/time"
)

var (
	//go:embed observation_schema.json
	jsonSchema []byte
)

type Observation struct {
	Time *spice_time.Time   `json:"time"`
	Data map[string]float64 `json:"data"`
	Tags []string           `json:"tags,omitempty"`
}

func JsonSchema() *[]byte {
	return &jsonSchema
}
