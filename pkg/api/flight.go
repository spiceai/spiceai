package api

import (
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

type Flight struct {
	Id        string                `json:"id"`
	Algorithm string                `json:"algorithm"`
	Loggers   []string              `json:"loggers"`
	Start     int64                 `json:"start"`
	End       int64                 `json:"end"`
	Episodes  []*runtime_pb.Episode `json:"episodes"`
}

func NewFlight(f *flights.Flight) *Flight {
	episodes := make([]*runtime_pb.Episode, 0)
	for _, ep := range f.Episodes() {
		episode := NewEpisode(ep)
		episodes = append(episodes, episode)
	}

	return &Flight{
		Id:        f.Id(),
		Algorithm: f.Algorithm(),
		Loggers:   f.Loggers(),
		Start:     f.Start().Unix(),
		End:       f.End().Unix(),
		Episodes:  episodes,
	}
}
