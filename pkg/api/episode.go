package api

import "github.com/spiceai/spice/pkg/flights"

type Episode struct {
	EpisodeId    uint64            `json:"episode"`
	Start        int64             `json:"start"`
	End          int64             `json:"end"`
	Score        float64           `json:"score"`
	ActionsTaken map[string]uint64 `json:"actions_taken"`
	Error        string            `json:"error"`
	ErrorMessage string            `json:"error_message"`
}

func NewEpisode(ep *flights.Episode) *Episode {
	return &Episode{
		EpisodeId:    ep.EpisodeId,
		Start:        ep.Start.Unix(),
		End:          ep.End.Unix(),
		Score:        ep.Score,
		ActionsTaken: ep.ActionsTaken,
	}
}
