package flights

import "time"

type Episode struct {
	EpisodeId    int64
	Start        time.Time
	End          time.Time
	Score        float64
	ActionsTaken map[string]uint64
	Error        string
	ErrorMessage string
}
