package interpretations

import (
	"fmt"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/util"
)

type InterpretationsStore struct {
	epoch       time.Time
	period      time.Duration
	granularity time.Duration
	intervals   int64

	interpretationsMutex sync.RWMutex
	interpretations      []Interpretation
	timeIndex            [][]*Interpretation
}

func NewInterpretationsStore(epoch time.Time, period time.Duration, granularity time.Duration) *InterpretationsStore {
	intervals := util.NumIntervals(period, granularity)
	return &InterpretationsStore{
		epoch:       epoch,
		period:      period,
		granularity: granularity,
		intervals: intervals,
		timeIndex: make([][]*Interpretation, 0, intervals),
	}
}

func (store *InterpretationsStore)Intervals() int64 {
	return store.intervals
}

func (store *InterpretationsStore) All() []Interpretation {
	return store.interpretations
}

func (store *InterpretationsStore) Get(start time.Time, end time.Time) []Interpretation {
	// naive linear filter - something smarter later
	store.interpretationsMutex.RLock()
	defer store.interpretationsMutex.RUnlock()

	var filteredInterpretations []Interpretation
	// At least one end falls within the range
	for _, i := range store.interpretations {
		if i.End().Before(start) {
			continue
		}
		if i.Start().After(end) {
			continue
		}
		filteredInterpretations = append(filteredInterpretations, i)
	}

	return filteredInterpretations
}

func (store *InterpretationsStore) Add(interpretation *Interpretation) error {
	if interpretation.Start().Before(store.epoch) {
		return fmt.Errorf("interpretation start '%s' must be same or later than pod epoch '%s'", interpretation.Start().String(), store.epoch.String())
	}

	store.interpretationsMutex.Lock()
	defer store.interpretationsMutex.Unlock()

	store.interpretations = append(store.interpretations, *interpretation)

	return nil
}