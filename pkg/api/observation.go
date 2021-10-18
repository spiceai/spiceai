package api

import (
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
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
