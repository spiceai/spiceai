package interpretations

import (
	"fmt"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/util"
)

type InterpretationsStore struct {
	epoch       time.Time
	endTime     time.Time
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
		endTime:     epoch.Add(period),
		period:      period,
		granularity: granularity,
		intervals:   intervals,
		timeIndex:   make([][]*Interpretation, intervals, intervals),
	}
}

func (store *InterpretationsStore) Intervals() int64 {
	return store.intervals
}

func (store *InterpretationsStore) All() []Interpretation {
	return store.interpretations
}

func (store *InterpretationsStore) TimeIndex() [][]*Interpretation {
	return store.timeIndex
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

	if interpretation.End().After(store.endTime) {
		return fmt.Errorf("interpretation end '%s' must be same or before pod end '%s'", interpretation.End().String(), store.endTime.String())
	}

	store.interpretationsMutex.Lock()
	store.interpretations = append(store.interpretations, *interpretation)
	store.addToTimeIndex(interpretation)
	store.interpretationsMutex.Unlock()

	return nil
}

func (store *InterpretationsStore) addToTimeIndex(interpretation *Interpretation) {
	startOffset := interpretation.Start().Sub(store.epoch)
	startIndex := startOffset / store.granularity
	intervals := interpretation.End().Sub(interpretation.Start()) / store.granularity
	for i := startIndex; i < intervals; i++ {
		store.timeIndex[i] = append(store.timeIndex[i], interpretation)
	}
}
