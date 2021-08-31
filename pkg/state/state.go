package state

import (
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/observations"
)

type State struct {
	Time               time.Time
	TimeSentToAIEngine time.Time
	path               string
	fieldNames         []string
	fields             []string
	observations       []observations.Observation
	observationsMutex  sync.RWMutex
}

func NewState(path string, fieldNames []string, observations []observations.Observation) *State {
	fields := make([]string, len(fieldNames))
	for i, name := range fieldNames {
		fields[i] = path + "." + name
	}

	return &State{
		Time:               time.Now(),
		TimeSentToAIEngine: time.Time{},
		path:               path,
		fieldNames:         fieldNames,
		fields:             fields,
		observations:       observations,
	}
}

func (s *State) Path() string {
	return s.path
}

func (s *State) FieldNames() []string {
	return s.fieldNames
}

func (s *State) Fields() []string {
	return s.fields
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
