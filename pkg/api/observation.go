package api

import (
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
	"github.com/spiceai/spiceai/pkg/state"
	spice_time "github.com/spiceai/spiceai/pkg/time"
)

type Observation struct {
	Time         *spice_time.Time   `json:"time"`
	Measurements map[string]float64 `json:"measurements"`
	Categories   map[string]string  `json:"categories"`
	Tags         []string           `json:"tags,omitempty"`
}

func NewObservation(o *observations.Observation) *common_pb.Observation {
	return &common_pb.Observation{
		Time:         o.Time,
		Measurements: o.Measurements,
		Categories:   o.Categories,
		Tags:         o.Tags,
	}
}

func NewObservationsFromState(s *state.State) []*common_pb.Observation {
	measurementsNameMap := s.MeasurementsNamesMap()

	apiObservations := []*common_pb.Observation{}
	for _, o := range s.Observations() {
		apiMeasurements := make(map[string]float64, len(o.Measurements))
		for measurementName, m := range measurementsNameMap {
			apiMeasurements[m] = o.Measurements[measurementName]
		}
		apiObservation := &common_pb.Observation{
			Time:         o.Time,
			Measurements: apiMeasurements,
			Tags:         o.Tags,
		}
		apiObservations = append(apiObservations, apiObservation)
	}

	return apiObservations
}
