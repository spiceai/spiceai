package api

import (
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
	spice_time "github.com/spiceai/spiceai/pkg/time"
)

type Observation struct {
	Time         *spice_time.Time   `json:"time"`
	Identifiers  map[string]string  `json:"identifiers"`
	Measurements map[string]float64 `json:"measurements"`
	Categories   map[string]string  `json:"categories"`
	Tags         []string           `json:"tags,omitempty"`
}

func NewObservation(o *observations.Observation) *common_pb.Observation {
	return &common_pb.Observation{
		Time:         o.Time,
		Identifiers:  o.Identifiers,
		Measurements: o.Measurements,
		Categories:   o.Categories,
		Tags:         o.Tags,
	}
}

// func NewObservationsFromState(s *state.State) []*common_pb.Observation {
// 	identifiersNamesMap := s.IdentifiersNamesMap()
// 	measurementsNameMap := s.MeasurementsNamesMap()
// 	categoryNameMap := s.CategoryNamesMap()

// 	apiObservations := []*common_pb.Observation{}
// 	for _, o := range s.Observations() {
// 		apiIdentifiers := make(map[string]string, len(o.Identifiers))
// 		for identifierName, i := range identifiersNamesMap {
// 			apiIdentifiers[i] = o.Identifiers[identifierName]
// 		}
// 		apiMeasurements := make(map[string]float64, len(o.Measurements))
// 		for measurementName, m := range measurementsNameMap {
// 			apiMeasurements[m] = o.Measurements[measurementName]
// 		}
// 		apiCategories := make(map[string]string, len(o.Categories))
// 		for categoryName, c := range categoryNameMap {
// 			apiCategories[c] = o.Categories[categoryName]
// 		}
// 		apiObservation := &common_pb.Observation{
// 			Time:         o.Time,
// 			Identifiers:  apiIdentifiers,
// 			Measurements: apiMeasurements,
// 			Categories:   apiCategories,
// 			Tags:         o.Tags,
// 		}
// 		apiObservations = append(apiObservations, apiObservation)
// 	}

// 	return apiObservations
// }
