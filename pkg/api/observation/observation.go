package observation

import (
	_ "embed"
	"time"
)

var (
	//go:embed observation_schema.json
	jsonSchema []byte
)

type Observation struct {
	Time *time.Time         `json:"time"`
	Data map[string]float64 `json:"data"`
	Tags []string           `json:"tags,omitempty"`
}

func JsonSchema() *[]byte {
	return &jsonSchema
}
