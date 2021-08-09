package state

import (
	"sync"
	"time"

	"github.com/spiceai/spice/pkg/observations"
)

type State struct {
	Time               time.Time
	TimeSentToAIEngine time.Time
	Fields             []string
	observations       []observations.Observation
	observationsMutex  sync.RWMutex
}

func NewState(fields []string, observations []observations.Observation) *State {
	return &State{
		Time:               time.Now(),
		TimeSentToAIEngine: time.Time{},
		Fields:             fields,
		observations:       observations,
	}
}

func (s *State) Observations() []observations.Observation {
	return s.observations
}

func (s *State) Sent() {
	s.TimeSentToAIEngine = time.Now()
}

func (s *State) AddData(newObservations ...observations.Observation) {
	s.observationsMutex.Lock()
	defer s.observationsMutex.Unlock()

	s.observations = append(s.observations, newObservations...)
}
