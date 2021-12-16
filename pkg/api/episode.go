package api

import (
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

func NewEpisode(ep *flights.Episode) *runtime_pb.Episode {
	return &runtime_pb.Episode{
		Episode:      ep.EpisodeId,
		Start:        ep.Start.Unix(),
		End:          ep.End.Unix(),
		Score:        ep.Score,
		ActionsTaken: ep.ActionsTaken,
		Error:        ep.Error,
		ErrorMessage: ep.ErrorMessage,
	}
}
