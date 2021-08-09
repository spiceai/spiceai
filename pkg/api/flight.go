package api

import "github.com/spiceai/spice/pkg/flights"

type Flight struct {
	Start    int64       `json:"start"`
	End      int64       `json:"end"`
	Episodes *[]*Episode `json:"episodes"`
}

func NewFlight(f *flights.Flight) *Flight {
	episodes := make([]*Episode, 0)
	for _, ep := range f.Episodes() {
		episode := NewEpisode(ep)
		episodes = append(episodes, episode)
	}

	return &Flight{
		Start:    f.Start().Unix(),
		End:      f.End().Unix(),
		Episodes: &episodes,
	}
}
