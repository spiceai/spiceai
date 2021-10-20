package pods

import "time"

type PodParams struct {
	Epoch             time.Time
	Period            time.Duration
	Interval          time.Duration
	Granularity       time.Duration
	LearningAlgorithm string
}

func NewPodParams() *PodParams {
	return &PodParams{
		Period:            time.Hour * 24 * 3,
		Interval:          time.Minute * 1,
		Granularity:       time.Second * 10,
		LearningAlgorithm: "dql",
	}
}
