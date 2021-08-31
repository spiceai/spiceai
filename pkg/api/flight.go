package api

import (
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

func NewFlight(f *flights.Flight) *runtime_pb.Flight {
	episodes := make([]*runtime_pb.Episode, 0)
	for _, ep := range f.Episodes() {
		episode := NewEpisode(ep)
		episodes = append(episodes, episode)
	}

	return &runtime_pb.Flight{
		Start:    f.Start().Unix(),
		End:      f.End().Unix(),
		Episodes: episodes,
	}
}
