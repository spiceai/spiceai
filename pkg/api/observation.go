package api

import (
	_ "embed"

	"github.com/spiceai/spiceai/pkg/util"
)

var (
	//go:embed observation_schema.json
	jsonSchema []byte
)

type Time struct {
	Integer *int64
	String  *string
}

type Observation struct {
	Time *Time              `json:"time"`
	Data map[string]float64 `json:"data"`
	Tags []string           `json:"tags,omitempty"`
}

func JsonSchema() *[]byte {
	return &schema
}

func (x *Time) UnmarshalJSON(data []byte) error {
	err := util.UnmarshalUnion(data, &x.Integer, &x.String)
	if err != nil {
		return err
	}
	return nil
}

func (x *Time) MarshalJSON() ([]byte, error) {
	return util.MarshalUnion(x.Integer, x.String)
}
