package observation

import (
	_ "embed"

	spice_json "github.com/spiceai/spiceai/pkg/json"
	spice_time "github.com/spiceai/spiceai/pkg/time"
)

var (
	//go:embed observation_schema.json
	jsonSchema []byte
)

type ObservationValue struct {
	String  *string
	Float64 *float64
}

type Observation struct {
	Time *spice_time.Time             `json:"time"`
	Data map[string]*ObservationValue `json:"data"`
	Tags []string                     `json:"tags,omitempty"`
}

func JsonSchema() []byte {
	return jsonSchema
}

func (x *ObservationValue) UnmarshalJSON(data []byte) error {
	err := spice_json.UnmarshalUnion(data, nil, &x.String, &x.Float64)
	if err != nil {
		return err
	}
	return nil
}

func (x *ObservationValue) MarshalJSON() ([]byte, error) {
	return spice_json.MarshalUnion(nil, x.String, x.Float64)
}
