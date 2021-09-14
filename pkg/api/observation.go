package api

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
)

var (
	//go:embed observation_schema.json
	schema []byte
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

type ObservationJsonFormat struct {
}

func (s *ObservationJsonFormat) GetSchema() *[]byte {
	return &schema
}

func (s *ObservationJsonFormat) GetObservations(data *[]byte) ([]observations.Observation, error) {
	var observationPoints []Observation = make([]Observation, 0)

	err := json.Unmarshal(*data, &observationPoints)
	if err != nil {
		return nil, err
	}

	var newObservations []observations.Observation
	for _, point := range observationPoints {
		var ts int64
		var err error

		if point.Time.Integer != nil {
			ts = *point.Time.Integer
		} else if point.Time.String != nil {
			var t time.Time
			t, err = time.Parse(time.RFC3339, *point.Time.String)
			if err != nil {
				// This should never happen as the schema validation would have caught this
				return nil, fmt.Errorf("observation time format is invalid: %s", *point.Time.String)
			}
			ts = t.Unix()
		} else {
			// This should never happen as the schema validation would have caught this
			return nil, fmt.Errorf("observation did not include a time component")
		}

		observation := observations.Observation{
			Time: ts,
			Data: point.Data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}

func (s *ObservationJsonFormat) GetState(data []byte, validFields *[]string) ([]*state.State, error) {
	// TODO
	return nil, nil
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
